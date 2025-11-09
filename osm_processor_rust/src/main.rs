use anyhow::{Context, Result};
use geo::{Distance, Haversine, Point};
use indicatif::{ProgressBar, ProgressStyle};
use osmpbf::{Element, ElementReader};
use polars::prelude::*;
use rayon::prelude::*;
use rstar::RTree;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A street segment from OSM
#[derive(Debug, Clone)]
struct StreetSegment {
    street_name: String,
    state: String,
    way_id: i64,
    node_ids: Vec<i64>,
    coords: Vec<(f64, f64)>, // (lat, lon)
    highway_type: String,
    tags: HashMap<String, String>,
}

impl StreetSegment {
    /// Get representative coordinates (first point)
    fn rep_coords(&self) -> (f64, f64) {
        self.coords[0]
    }
}

/// A unique street (potentially multiple segments grouped together)
#[derive(Debug)]
struct Street {
    street_name: String,
    state: String,
    lat: f64,
    lon: f64,
    num_segments: usize,
    highway_type: String,
    tags: HashMap<String, String>,
}

/// First pass: collect which nodes are used by named highways
fn collect_highway_nodes(pbf_path: &Path) -> Result<HashSet<i64>> {
    println!("Pass 1: Identifying nodes used by named highways...");
    
    let reader = ElementReader::from_path(pbf_path)
        .context("Failed to open OSM file")?;
    
    let mut highway_nodes = HashSet::new();
    let mut way_count = 0;
    
    reader.for_each(|element| {
        if let Element::Way(way) = element {
            let tags: HashMap<_, _> = way.tags().collect();
            
            // Check if this way has both a name and is a highway
            if tags.contains_key("name") && tags.contains_key("highway") {
                way_count += 1;
                for node_id in way.refs() {
                    highway_nodes.insert(node_id);
                }
            }
        }
    })?;
    
    println!("  Found {} named highways using {} nodes", way_count, highway_nodes.len());
    Ok(highway_nodes)
}

/// Second pass: extract street segments with coordinates
fn extract_street_segments(
    pbf_path: &Path,
    state_name: &str,
    highway_nodes: &HashSet<i64>,
) -> Result<Vec<StreetSegment>> {
    println!("Pass 2: Extracting street segments...");
    
    // First pass through file: collect node coordinates
    println!("  Loading node coordinates...");
    let reader = ElementReader::from_path(pbf_path)
        .context("Failed to open OSM file")?;
    
    // Use par_map_reduce to collect nodes in parallel
    let (node_coords, node_count, matched_count) = reader.par_map_reduce(
        |element| {
            let mut coords = HashMap::new();
            let mut total = 0;
            let mut matched = 0;
            
            match element {
                Element::Node(node) => {
                    total = 1;
                    if highway_nodes.contains(&node.id()) {
                        matched = 1;
                        coords.insert(node.id(), (node.lat(), node.lon()));
                    }
                }
                Element::DenseNode(node) => {
                    total = 1;
                    if highway_nodes.contains(&node.id()) {
                        matched = 1;
                        coords.insert(node.id(), (node.lat(), node.lon()));
                    }
                }
                _ => {}
            }
            
            (coords, total, matched)
        },
        || (HashMap::new(), 0, 0),
        |mut a, b| {
            a.0.extend(b.0);
            a.1 += b.1;
            a.2 += b.2;
            a
        },
    )?;
    
    println!("  Scanned {} nodes, matched {} highway nodes, loaded {} coordinates", 
             node_count, matched_count, node_coords.len());
    
    // Second pass through file: extract ways
    println!("  Extracting ways...");
    let reader = ElementReader::from_path(pbf_path)
        .context("Failed to open OSM file")?;
    
    let mut segments = Vec::new();
    
    reader.for_each(|element| {
        if let Element::Way(way) = element {
            let tags: HashMap<String, String> = way
                .tags()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            
            if let (Some(name), Some(highway_type)) = (tags.get("name"), tags.get("highway")) {
                // Collect coordinates for this way
                let coords: Vec<(f64, f64)> = way
                    .refs()
                    .filter_map(|node_id| node_coords.get(&node_id).copied())
                    .collect();
                
                if !coords.is_empty() {
                    segments.push(StreetSegment {
                        street_name: name.clone(),
                        state: state_name.to_string(),
                        way_id: way.id(),
                        node_ids: way.refs().collect(),
                        coords,
                        highway_type: highway_type.clone(),
                        tags,
                    });
                }
            }
        }
    })?;
    
    println!("  Found {} street segments", segments.len());
    Ok(segments)
}

/// Group segments into connected components using node sharing
fn find_connected_components(segments: &[StreetSegment]) -> Vec<Vec<usize>> {
    if segments.is_empty() {
        return Vec::new();
    }
    
    let n = segments.len();
    
    // Build adjacency list based on shared nodes
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    
    // For efficiency, build a map of node_id -> segment indices
    let mut node_to_segments: HashMap<i64, Vec<usize>> = HashMap::new();
    for (i, seg) in segments.iter().enumerate() {
        for &node_id in &seg.node_ids {
            node_to_segments.entry(node_id).or_default().push(i);
        }
    }
    
    // Connect segments that share nodes
    for segment_indices in node_to_segments.values() {
        for i in 0..segment_indices.len() {
            for j in (i + 1)..segment_indices.len() {
                let idx_i = segment_indices[i];
                let idx_j = segment_indices[j];
                adj[idx_i].push(idx_j);
                adj[idx_j].push(idx_i);
            }
        }
    }
    
    // Find connected components using BFS
    let mut visited = vec![false; n];
    let mut components = Vec::new();
    
    for start in 0..n {
        if !visited[start] {
            let mut component = Vec::new();
            let mut queue = vec![start];
            visited[start] = true;
            
            while let Some(current) = queue.pop() {
                component.push(current);
                for &neighbor in &adj[current] {
                    if !visited[neighbor] {
                        visited[neighbor] = true;
                        queue.push(neighbor);
                    }
                }
            }
            
            components.push(component);
        }
    }
    
    components
}

/// Group segments with same name using spatial proximity (for disconnected segments)
/// Matches Python algorithm: checks minimum distance between ANY nodes in components
fn group_nearby_components(
    segments: &[StreetSegment],
    components: Vec<Vec<usize>>,
    distance_threshold_km: f64,
) -> Vec<Vec<usize>> {
    if components.len() <= 1 {
        return components;
    }
    
    // Build connectivity graph based on distance threshold
    // This matches the Python algorithm exactly
    let n = components.len();
    let mut connections: Vec<Vec<usize>> = vec![Vec::new(); n];
    
    // Progress bar for distance checks
    let pb = ProgressBar::new((n * (n - 1) / 2) as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  [{bar:40}] {pos}/{len} pairs ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    
    // Check all pairs of components (quadratic, like Python)
    for i in 0..n {
        for j in (i + 1)..n {
            pb.inc(1);
            // Check minimum distance between any nodes in the two components
            let mut min_dist = f64::INFINITY;
            
            for &seg_i in &components[i] {
                for &(lat1, lon1) in &segments[seg_i].coords {
                    for &seg_j in &components[j] {
                        for &(lat2, lon2) in &segments[seg_j].coords {
                            // Simple approximation: 1 degree ≈ 111 km
                            let dlat = lat2 - lat1;
                            let dlon = lon2 - lon1;
                            let dist = (dlat * dlat + dlon * dlon).sqrt() * 111.0;
                            min_dist = min_dist.min(dist);
                        }
                    }
                }
            }
            
            if min_dist < distance_threshold_km {
                connections[i].push(j);
                connections[j].push(i);
            }
        }
    }
    
    pb.finish_and_clear();
    
    // Find connected components using BFS (same as Python)
    let mut visited = vec![false; n];
    let mut final_components = Vec::new();
    
    for start in 0..n {
        if !visited[start] {
            let mut merged_component = Vec::new();
            let mut queue = vec![start];
            visited[start] = true;
            
            while let Some(current) = queue.pop() {
                // Add all segments from this component
                merged_component.extend(&components[current]);
                
                for &neighbor in &connections[current] {
                    if !visited[neighbor] {
                        visited[neighbor] = true;
                        queue.push(neighbor);
                    }
                }
            }
            
            final_components.push(merged_component);
        }
    }
    
    final_components
}

/// Group segments into unique streets
fn group_segments_into_streets(
    segments: Vec<StreetSegment>,
    distance_threshold_km: f64,
) -> Vec<Street> {
    println!("Grouping segments into unique streets...");
    
    // Group by (name, state)
    let mut by_name_state: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (i, seg) in segments.iter().enumerate() {
        let key = (seg.street_name.clone(), seg.state.clone());
        by_name_state.entry(key).or_default().push(i);
    }
    
    println!("  Found {} unique street names", by_name_state.len());
    
    // Progress bar for processing street names
    let pb = ProgressBar::new(by_name_state.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  Processing: [{bar:40}] {pos}/{len} street names ({eta})")
            .unwrap()
            .progress_chars("=>-"),
    );
    
    // Process each name group in parallel
    let streets: Vec<Street> = by_name_state
        .into_par_iter()
        .flat_map(|((name, state), indices)| {
            let name_segments: Vec<_> = indices.iter().map(|&i| segments[i].clone()).collect();
            
            // Find connected components
            let components = find_connected_components(&name_segments);
            
            // Optionally merge nearby components
            let final_components = if distance_threshold_km > 0.0 {
                group_nearby_components(&name_segments, components, distance_threshold_km)
            } else {
                components
            };
            
            pb.inc(1);
            
            // Create one street per component
            final_components
                .into_iter()
                .map(|component_indices| {
                    let segs: Vec<_> = component_indices
                        .iter()
                        .map(|&i| &name_segments[i])
                        .collect();
                    
                    // Use first segment's coordinates
                    let (lat, lon) = segs[0].rep_coords();
                    
                    // Most common highway type
                    let highway_type = segs
                        .iter()
                        .map(|s| s.highway_type.as_str())
                        .max_by_key(|&ht| segs.iter().filter(|s| s.highway_type == ht).count())
                        .unwrap_or("")
                        .to_string();
                    
                    // Collect common tags (appear in >50% of segments)
                    let mut tag_counts: HashMap<String, usize> = HashMap::new();
                    for seg in &segs {
                        for key in seg.tags.keys() {
                            *tag_counts.entry(key.clone()).or_default() += 1;
                        }
                    }
                    
                    let threshold = segs.len() / 2;
                    let mut common_tags = HashMap::new();
                    for (key, count) in tag_counts {
                        if count >= threshold {
                            // Find most common value for this key
                            let mut value_counts: HashMap<String, usize> = HashMap::new();
                            for seg in &segs {
                                if let Some(value) = seg.tags.get(&key) {
                                    *value_counts.entry(value.clone()).or_default() += 1;
                                }
                            }
                            if let Some((value, _)) = value_counts.iter().max_by_key(|(_, &c)| c) {
                                common_tags.insert(key, value.clone());
                            }
                        }
                    }
                    
                    Street {
                        street_name: name.clone(),
                        state: state.clone(),
                        lat,
                        lon,
                        num_segments: segs.len(),
                        highway_type,
                        tags: common_tags,
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect();
    
    pb.finish_and_clear();
    
    println!("  Created {} unique streets", streets.len());
    streets
}

/// Convert streets to Polars DataFrame
fn streets_to_dataframe(streets: Vec<Street>) -> Result<DataFrame> {
    let street_names: Vec<String> = streets.iter().map(|s| s.street_name.clone()).collect();
    let states: Vec<String> = streets.iter().map(|s| s.state.clone()).collect();
    let lats: Vec<f64> = streets.iter().map(|s| s.lat).collect();
    let lons: Vec<f64> = streets.iter().map(|s| s.lon).collect();
    let num_segments: Vec<u32> = streets.iter().map(|s| s.num_segments as u32).collect();
    let highway_types: Vec<String> = streets.iter().map(|s| s.highway_type.clone()).collect();
    
    let df = DataFrame::new(vec![
        Series::new("street_name", street_names),
        Series::new("state", states),
        Series::new("lat", lats),
        Series::new("lon", lons),
        Series::new("num_segments", num_segments),
        Series::new("highway_type", highway_types),
    ])?;
    
    Ok(df)
}

/// Main processing function
fn process_osm_to_parquet(
    pbf_path: &Path,
    state_name: &str,
    output_path: Option<PathBuf>,
    distance_threshold_km: f64,
) -> Result<()> {
    println!("\n{}", "=".repeat(70));
    println!("OSM TO PARQUET PROCESSOR (Rust)");
    println!("{}", "=".repeat(70));
    println!("Input file:  {}", pbf_path.display());
    println!("State:       {}", state_name);
    println!("Distance threshold: {} km", distance_threshold_km);
    println!("{}", "=".repeat(70));
    
    // Determine output path
    let output_path = output_path.unwrap_or_else(|| {
        let mut path = pbf_path.parent().unwrap().parent().unwrap().to_path_buf();
        path.push("streetdfs");
        std::fs::create_dir_all(&path).ok();
        path.push(format!("{}_streets.parquet", state_name));
        path
    });
    
    // Two-pass processing
    let highway_nodes = collect_highway_nodes(pbf_path)?;
    let segments = extract_street_segments(pbf_path, state_name, &highway_nodes)?;
    
    // Group into streets
    let streets = group_segments_into_streets(segments, distance_threshold_km);
    
    // Convert to DataFrame
    println!("Creating DataFrame...");
    let df = streets_to_dataframe(streets)?;
    
    // Show statistics
    println!("\n{}", "=".repeat(70));
    println!("SUMMARY STATISTICS");
    println!("{}", "=".repeat(70));
    println!("Total unique streets: {}", df.height());
    
    let multi_segment = df
        .clone()
        .lazy()
        .filter(col("num_segments").gt(lit(1)))
        .collect()?;
    println!("Streets with multiple segments: {}", multi_segment.height());
    
    // Top street names
    println!("\nTop 10 street names:");
    let name_counts = df
        .clone()
        .lazy()
        .group_by([col("street_name")])
        .agg([len().alias("count")])
        .sort(["count"], Default::default())
        .reverse()
        .limit(10)
        .collect()?;
    println!("{}", name_counts);
    
    // Save to parquet
    println!("\nSaving to: {}", output_path.display());
    let mut file = std::fs::File::create(&output_path)?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;
    
    println!("Done!");
    println!("{}", "=".repeat(70));
    
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: {} <state_name> [pbf_file] [distance_threshold_km]", args[0]);
        eprintln!("Example: {} delaware", args[0]);
        eprintln!("Example: {} california /path/to/california-latest.osm.pbf 0.1", args[0]);
        std::process::exit(1);
    }
    
    let state_name = args[1].to_lowercase();
    
    let pbf_path = if args.len() > 2 {
        PathBuf::from(&args[2])
    } else {
        // Default: look in data/osm directory
        let mut path = std::env::current_dir()?;
        path.push("data");
        path.push("osm");
        path.push(format!("{}-latest.osm.pbf", state_name));
        path
    };
    
    let distance_threshold_km = if args.len() > 3 {
        args[3].parse().context("Invalid distance threshold")?
    } else {
        0.2 // Default 200m
    };
    
    if !pbf_path.exists() {
        anyhow::bail!("File not found: {}", pbf_path.display());
    }
    
    process_osm_to_parquet(&pbf_path, &state_name, None, distance_threshold_km)?;
    
    Ok(())
}
