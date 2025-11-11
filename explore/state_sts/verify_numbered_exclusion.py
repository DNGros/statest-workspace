#!/usr/bin/env python3
"""Verify that all state-named street plotting scripts exclude numbered streets."""

import sys
from pathlib import Path
import ast
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def check_script_excludes_numbered(script_path: Path) -> tuple[bool, str]:
    """
    Check if a script excludes numbered streets.
    Returns (is_correct, explanation)
    """
    content = script_path.read_text()
    
    # Check if it uses load_state_streets_df (which excludes numbered by default)
    uses_load_state_streets = 'load_state_streets_df' in content
    
    # Check if it explicitly sets exclude_numbered=False
    has_exclude_false = re.search(r'exclude_numbered\s*=\s*False', content, re.IGNORECASE)
    
    # Check if it manually filters numbered streets
    has_manual_filter = re.search(r'~.*street_name.*contains.*\\d', content) or \
                       re.search(r'filter.*street_name.*contains.*\\d', content)
    
    # Check if it uses load_street_df without filtering
    uses_load_street_df = 'load_street_df' in content and 'load_state_streets_df' not in content
    
    if uses_load_state_streets:
        if has_exclude_false:
            return False, "Uses load_state_streets_df but sets exclude_numbered=False"
        return True, "Uses load_state_streets_df() which excludes numbered streets by default"
    
    if uses_load_street_df:
        if has_manual_filter:
            return True, "Uses load_street_df() but manually filters numbered streets"
        return False, "Uses load_street_df() without filtering numbered streets"
    
    return None, "Could not determine data loading method"


def main():
    """Check all state-named street plotting scripts."""
    # Script is in workspace/explore/state_sts/, so parent.parent.parent is workspace root
    workspace_dir = Path(__file__).parent.parent.parent
    state_sts_dir = workspace_dir / "explore" / "state_sts"
    
    scripts_to_check = [
        state_sts_dir / "most_common_state_st.py",
        state_sts_dir / "most_common_state_st_stacked.py",
        state_sts_dir / "most_common_state_st_stacked_all.py",
        state_sts_dir / "analyze_state_naming_fractions.py",
    ]
    
    print("="*80)
    print("VERIFYING NUMBERED STREET EXCLUSION IN PLOTTING SCRIPTS")
    print("="*80)
    print()
    
    all_correct = True
    
    for script_path in scripts_to_check:
        if not script_path.exists():
            print(f"⚠️  {script_path.name}: FILE NOT FOUND")
            continue
        
        is_correct, explanation = check_script_excludes_numbered(script_path)
        
        if is_correct is True:
            print(f"✅ {script_path.name}")
            print(f"   {explanation}")
        elif is_correct is False:
            print(f"❌ {script_path.name}")
            print(f"   {explanation}")
            all_correct = False
        else:
            print(f"⚠️  {script_path.name}")
            print(f"   {explanation}")
            all_correct = False
        
        print()
    
    print("="*80)
    if all_correct:
        print("✅ ALL SCRIPTS CORRECTLY EXCLUDE NUMBERED STREETS")
    else:
        print("❌ SOME SCRIPTS NEED FIXING")
    print("="*80)
    
    # Also verify the load_state_streets_df function
    print("\nVerifying load_state_streets_df() default behavior...")
    from workspace.load_street_df import load_state_streets_df
    import inspect
    
    sig = inspect.signature(load_state_streets_df)
    exclude_numbered_param = sig.parameters.get('exclude_numbered')
    
    if exclude_numbered_param and exclude_numbered_param.default is True:
        print("✅ load_state_streets_df() defaults to exclude_numbered=True")
    else:
        print("❌ load_state_streets_df() does NOT default to exclude numbered streets")
        all_correct = False
    
    return 0 if all_correct else 1


if __name__ == "__main__":
    sys.exit(main())

