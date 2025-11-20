#!/usr/bin/env python3
"""
Memory profiling script v2 to compare telemetry ON vs OFF footprint.

This version properly isolates the telemetry-specific memory overhead
by ensuring imports are loaded before measurement begins.
"""

import tracemalloc
import gc
import sys
import os
import time
import json

# Environment variables should be set before running this script:
# export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
# export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
# export DATABRICKS_TOKEN="your-token"

# Verify environment variables are set
required_env_vars = ['DATABRICKS_SERVER_HOSTNAME', 'DATABRICKS_HTTP_PATH', 'DATABRICKS_TOKEN']
missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
if missing_vars:
    print(f"❌ Error: Missing required environment variables: {', '.join(missing_vars)}")
    print("\nPlease set them using:")
    print("  export DATABRICKS_SERVER_HOSTNAME='your-workspace.cloud.databricks.com'")
    print("  export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/your-warehouse-id'")
    print("  export DATABRICKS_TOKEN='your-token'")
    sys.exit(1)

# Add paths
sys.path.insert(0, '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/src')
sys.path.insert(0, '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python')

# Pre-import all modules to avoid counting import overhead
print("🔄 Pre-loading all modules...")
import databricks.sql as sql
from databricks.sql.telemetry.telemetry_client import TelemetryClient, NoopTelemetryClient
gc.collect()
print("✅ Modules loaded\n")


def format_bytes(size_bytes):
    """Format bytes to human readable format."""
    if size_bytes < 0:
        return "-" + format_bytes(-size_bytes)
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024**2):.2f} MB"


def run_queries_with_telemetry(enable_telemetry, num_cycles=10, queries_per_cycle=5):
    """
    Run queries with specified telemetry setting.
    
    Args:
        enable_telemetry: Boolean for telemetry state
        num_cycles: Number of connection cycles
        queries_per_cycle: Queries per connection
    
    Returns:
        dict: Statistics including memory and query counts
    """
    mode = f"Telemetry {'ENABLED' if enable_telemetry else 'DISABLED'}"
    print(f"\n{'='*80}")
    print(f"Testing: {mode}")
    print(f"{'='*80}\n")
    
    # Garbage collect and get baseline
    gc.collect()
    time.sleep(0.5)
    
    # Start tracking
    tracemalloc.start()
    baseline_current, baseline_peak = tracemalloc.get_traced_memory()
    
    connection_params = {
        'server_hostname': os.environ['DATABRICKS_SERVER_HOSTNAME'],
        'http_path': os.environ['DATABRICKS_HTTP_PATH'],
        'access_token': os.environ['DATABRICKS_TOKEN'],
        'enable_telemetry': enable_telemetry,
    }
    
    test_count = 0
    error_count = 0
    
    print(f"📊 Running {num_cycles} connection cycles × {queries_per_cycle} queries...\n")
    
    try:
        for cycle in range(num_cycles):
            print(f"  Cycle {cycle + 1}/{num_cycles}...", end=" ")
            
            try:
                conn = sql.connect(**connection_params)
                cursor = conn.cursor()
                
                for _ in range(queries_per_cycle):
                    try:
                        cursor.execute("SELECT 1 as test, 'hello' as msg, 42.0 as num")
                        result = cursor.fetchall()
                        test_count += 1
                    except Exception as e:
                        error_count += 1
                
                cursor.close()
                conn.close()
                
                current, peak = tracemalloc.get_traced_memory()
                print(f"Memory: {format_bytes(current)}")
                
            except Exception as e:
                print(f"ERROR: {e}")
                error_count += 1
            
            # Small pause between cycles
            time.sleep(0.1)
        
        # Final garbage collection
        gc.collect()
        time.sleep(0.5)
        
        # Get final memory snapshot
        snapshot = tracemalloc.take_snapshot()
        final_current, final_peak = tracemalloc.get_traced_memory()
        
        # Stop tracking
        tracemalloc.stop()
        
        # Analyze allocations
        top_stats = snapshot.statistics('lineno')
        
        # Find telemetry-related allocations
        telemetry_stats = [s for s in top_stats if 'telemetry' in s.traceback[0].filename.lower()]
        telemetry_total_bytes = sum(s.size for s in telemetry_stats)
        
        print(f"\n{'-'*80}")
        print(f"Results for: {mode}")
        print(f"{'-'*80}")
        print(f"Queries executed: {test_count}")
        print(f"Errors: {error_count}")
        print(f"\nMemory Usage (relative to baseline):")
        print(f"  Current: {format_bytes(final_current - baseline_current)}")
        print(f"  Peak:    {format_bytes(final_peak - baseline_peak)}")
        
        if telemetry_stats:
            print(f"\nTelemetry-Specific Allocations: {len(telemetry_stats)} locations")
            print(f"Total telemetry memory: {format_bytes(telemetry_total_bytes)}")
            print(f"\nTop 5 telemetry allocations:")
            for i, stat in enumerate(telemetry_stats[:5], 1):
                frame = stat.traceback[0]
                filename = frame.filename.replace('/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/', '')
                print(f"  🔴 {filename}:{frame.lineno}")
                print(f"      {format_bytes(stat.size)}, {stat.count:,} allocations")
        
        print(f"\nTop 10 Overall Allocations:")
        for i, stat in enumerate(top_stats[:10], 1):
            frame = stat.traceback[0]
            filename = frame.filename.replace('/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/', '')
            marker = "🔴" if 'telemetry' in filename.lower() else "  "
            print(f"{marker} {i:2d}. {filename}:{frame.lineno}")
            print(f"      {format_bytes(stat.size)}, {stat.count:,} allocations")
        
        return {
            "mode": mode,
            "enable_telemetry": enable_telemetry,
            "test_count": test_count,
            "error_count": error_count,
            "baseline_current": baseline_current,
            "baseline_peak": baseline_peak,
            "final_current": final_current,
            "final_peak": final_peak,
            "net_current": final_current - baseline_current,
            "net_peak": final_peak - baseline_peak,
            "telemetry_locations": len(telemetry_stats),
            "telemetry_bytes": telemetry_total_bytes,
        }
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        tracemalloc.stop()
        raise


def main():
    """Main comparison function."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║           TELEMETRY MEMORY FOOTPRINT ANALYSIS (V2 - Improved)                ║
║                                                                              ║
║  Compares incremental memory usage with telemetry ON vs OFF                  ║
║  All modules pre-loaded to isolate telemetry-specific overhead              ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Run telemetry OFF
        print("\n🔴 PHASE 1: Telemetry DISABLED")
        result_off = run_queries_with_telemetry(enable_telemetry=False, num_cycles=10, queries_per_cycle=5)
        
        # Pause and clean up
        gc.collect()
        time.sleep(2)
        
        # Run telemetry ON  
        print("\n🟢 PHASE 2: Telemetry ENABLED")
        result_on = run_queries_with_telemetry(enable_telemetry=True, num_cycles=10, queries_per_cycle=5)
        
        # Compare
        print(f"\n{'='*96}")
        print("MEMORY FOOTPRINT COMPARISON")
        print(f"{'='*96}\n")
        
        print(f"{'Metric':<45} {'OFF':<20} {'ON':<20} {'Difference':<11}")
        print(f"{'-'*96}")
        
        peak_diff = result_on['net_peak'] - result_off['net_peak']
        current_diff = result_on['net_current'] - result_off['net_current']
        
        print(f"{'Peak Memory (net)':<45} "
              f"{format_bytes(result_off['net_peak']):<20} "
              f"{format_bytes(result_on['net_peak']):<20} "
              f"{format_bytes(peak_diff)}")
        
        print(f"{'Current Memory (net)':<45} "
              f"{format_bytes(result_off['net_current']):<20} "
              f"{format_bytes(result_on['net_current']):<20} "
              f"{format_bytes(current_diff)}")
        
        print(f"{'Queries Executed':<45} "
              f"{result_off['test_count']:<20} "
              f"{result_on['test_count']:<20} "
              f"{result_on['test_count'] - result_off['test_count']}")
        
        if result_on['telemetry_bytes'] > 0:
            print(f"{'Telemetry-Specific Allocations':<45} "
                  f"{'N/A':<20} "
                  f"{format_bytes(result_on['telemetry_bytes']):<20} "
                  f"{format_bytes(result_on['telemetry_bytes'])}")
        
        print(f"\n{'-'*96}")
        print("ANALYSIS:")
        print(f"{'-'*96}")
        
        if result_off['net_peak'] > 0:
            percentage = (peak_diff / result_off['net_peak']) * 100
            print(f"\n📊 Telemetry overhead (peak): {format_bytes(peak_diff)} ({percentage:+.1f}%)")
        
        if result_off['net_current'] > 0:
            percentage = (current_diff / result_off['net_current']) * 100
            print(f"📊 Telemetry overhead (current): {format_bytes(current_diff)} ({percentage:+.1f}%)")
        
        abs_peak_mb = abs(peak_diff) / (1024**2)
        print(f"\n💡 Assessment:")
        if abs_peak_mb < 1:
            print("   ✅ Telemetry has MINIMAL memory overhead (< 1 MB)")
        elif abs_peak_mb < 5:
            print("   ✅ Telemetry has LOW memory overhead (1-5 MB)")
        elif abs_peak_mb < 20:
            print("   ⚠️  Telemetry has MODERATE memory overhead (5-20 MB)")
        else:
            print("   🚨 Telemetry has HIGH memory overhead (> 20 MB)")
        
        # Save results
        results_file = '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/memory_profile_results_v2.json'
        with open(results_file, 'w') as f:
            json.dump({
                "telemetry_off": result_off,
                "telemetry_on": result_on,
                "comparison": {
                    "peak_diff_bytes": peak_diff,
                    "current_diff_bytes": current_diff,
                    "telemetry_bytes": result_on['telemetry_bytes']
                }
            }, f, indent=2)
        
        print(f"\n💾 Results saved to: {results_file}")
        print(f"\n{'='*96}")
        print("✅ PROFILING COMPLETE")
        print(f"{'='*96}\n")
        
    except Exception as e:
        print(f"\n❌ Profiling failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

