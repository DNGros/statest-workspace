"""Interactive queries for president street data."""

from pathlib import Path
import polars as pl
import sys

def load_results():
    """Load the analysis results."""
    results_dir = Path(__file__).parent
    overall = pl.read_csv(results_dir / "president_streets_overall.csv")
    by_state = pl.read_csv(results_dir / "president_streets_by_state.csv")
    return overall, by_state


def query_president(president_name, by_state):
    """Show all states with streets named after a specific president."""
    results = (
        by_state
        .filter(pl.col("president_surname").str.to_lowercase() == president_name.lower())
        .sort("street_count", descending=True)
    )
    
    if len(results) == 0:
        print(f"No data found for president: {president_name}")
        return
    
    total = results["street_count"].sum()
    print(f"\n{president_name.title()} Streets: {total:,} total")
    print("=" * 60)
    
    for i, row in enumerate(results.iter_rows(named=True), 1):
        state_name = row["state"].replace("-", " ").title()
        pct = (row["street_count"] / total) * 100
        print(f"{i:2d}. {state_name:20s} {row['street_count']:4,} streets ({pct:5.1f}%)")


def query_state(state_name, by_state):
    """Show all president streets in a specific state."""
    # Normalize state name
    state_normalized = state_name.lower().replace(" ", "-")
    
    results = (
        by_state
        .filter(pl.col("state") == state_normalized)
        .sort("street_count", descending=True)
    )
    
    if len(results) == 0:
        print(f"No data found for state: {state_name}")
        return
    
    total = results["street_count"].sum()
    print(f"\n{state_name.title()} President Streets: {total:,} total")
    print("=" * 60)
    
    for i, row in enumerate(results.iter_rows(named=True), 1):
        pct = (row["street_count"] / total) * 100
        print(f"{i:2d}. {row['president_surname']:15s} {row['street_count']:4,} streets ({pct:5.1f}%)")


def compare_presidents(president_names, by_state):
    """Compare multiple presidents across states."""
    print(f"\nComparing: {', '.join(president_names)}")
    print("=" * 80)
    
    for president in president_names:
        results = (
            by_state
            .filter(pl.col("president_surname").str.to_lowercase() == president.lower())
            .sort("street_count", descending=True)
        )
        
        total = results["street_count"].sum()
        top_state = results.head(1).row(0, named=True) if len(results) > 0 else None
        
        if top_state:
            state_name = top_state["state"].replace("-", " ").title()
            print(f"{president.title():15s} {total:6,} total | Top: {state_name} ({top_state['street_count']:,})")
        else:
            print(f"{president.title():15s} No data")


def main():
    overall, by_state = load_results()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python query_president_streets.py president <name>")
        print("  python query_president_streets.py state <name>")
        print("  python query_president_streets.py compare <name1> <name2> ...")
        print("\nExamples:")
        print("  python query_president_streets.py president washington")
        print("  python query_president_streets.py state california")
        print("  python query_president_streets.py compare washington lincoln jefferson")
        return
    
    command = sys.argv[1].lower()
    
    if command == "president" and len(sys.argv) >= 3:
        query_president(sys.argv[2], by_state)
    
    elif command == "state" and len(sys.argv) >= 3:
        state_name = " ".join(sys.argv[2:])
        query_state(state_name, by_state)
    
    elif command == "compare" and len(sys.argv) >= 3:
        compare_presidents(sys.argv[2:], by_state)
    
    else:
        print("Invalid command or missing arguments")
        print("Use: president <name>, state <name>, or compare <name1> <name2> ...")


if __name__ == "__main__":
    main()


