#!/usr/bin/env python3
"""
Migration Runner Script
Executes both update_db.py and update_db2.py sequentially
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and handle its output."""
    print(f"\n{'='*60}")
    print(f"Starting: {description}")
    print(f"Script: {script_name}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    # Check if script exists
    if not os.path.exists(script_name):
        print(f"ERROR: Script '{script_name}' not found!")
        return False
    
    try:
        # Run the script
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, 
                              text=True, 
                              timeout=3600)  # 1 hour timeout
        
        # Print the output
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        # Check return code
        if result.returncode == 0:
            print(f"\n✅ SUCCESS: {description} completed successfully!")
            return True
        else:
            print(f"\n❌ FAILED: {description} failed with return code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n⏰ TIMEOUT: {description} timed out after 1 hour")
        return False
    except Exception as e:
        print(f"\n💥 ERROR: Failed to run {description}: {str(e)}")
        return False

def main():
    """Main function to run both migration scripts."""
    print("🚀 Starting MongoDB Migration Process")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track overall success
    overall_success = True
    
    # Script configurations
    scripts = [
        {
            'name': 'update_db.py',
            'description': 'MongoDB to MongoDB Migration (Database Migration)'
        },
        {
            'name': 'update_db2.py', 
            'description': 'CSV to MongoDB Migration (NFT Events Migration)'
        }
    ]
    
    # Run each script
    for script_config in scripts:
        script_name = script_config['name']
        description = script_config['description']
        
        success = run_script(script_name, description)
        
        if not success:
            overall_success = False
            print(f"\n⚠️  WARNING: {script_name} failed!")
            
            # Ask user if they want to continue
            try:
                response = input("Do you want to continue with the next migration? (y/n): ").lower().strip()
                if response not in ['y', 'yes']:
                    print("Migration process stopped by user.")
                    break
            except KeyboardInterrupt:
                print("\nMigration process interrupted by user.")
                break
    
    # Final summary
    print(f"\n{'='*60}")
    print("MIGRATION PROCESS SUMMARY")
    print(f"{'='*60}")
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if overall_success:
        print("🎉 ALL MIGRATIONS COMPLETED SUCCESSFULLY!")
    else:
        print("⚠️  SOME MIGRATIONS FAILED - CHECK LOGS ABOVE")
    
    return overall_success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n🛑 Migration process interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n💥 Unexpected error in migration runner: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)