#!/usr/bin/env python3
"""
Memory profiling script to compare telemetry ON vs OFF footprint.

This script runs E2E telemetry tests with tracemalloc to measure memory usage
and compares the footprint when telemetry is enabled vs disabled.
"""

import tracemalloc
import gc
import sys
import os

# Add the src directory to path
sys.path.insert(0, '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/src')
sys.path.insert(0, '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python')

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


def format_bytes(size_bytes):
    """Format bytes to human readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024**2):.2f} MB"


def run_telemetry_tests(enable_telemetry, force_enable=False):
    """
    Run telemetry E2E tests with memory tracking.
    
    Args:
        enable_telemetry: Value for enable_telemetry parameter
        force_enable: Value for force_enable_telemetry parameter
    
    Returns:
        dict: Memory statistics
    """
    from unittest.mock import patch
    import databricks.sql as sql
    
    mode = f"Telemetry {'ENABLED' if enable_telemetry else 'DISABLED'}"
    if force_enable:
        mode += " (FORCED)"
    
    print(f"\n{'='*80}")
    print(f"Running tests with: {mode}")
    print(f"{'='*80}\n")
    
    # Start memory tracking
    gc.collect()  # Clean up before starting
    tracemalloc.start()
    
    initial_current, initial_peak = tracemalloc.get_traced_memory()
    
    test_count = 0
    error_count = 0
    
    try:
        # Get connection parameters
        connection_params = {
            'server_hostname': os.environ['DATABRICKS_SERVER_HOSTNAME'],
            'http_path': os.environ['DATABRICKS_HTTP_PATH'],
            'access_token': os.environ['DATABRICKS_TOKEN'],
            'enable_telemetry': enable_telemetry,
        }
        
        if force_enable:
            connection_params['force_enable_telemetry'] = True
        
        print(f"📊 Running multiple query cycles to measure memory footprint...\n")
        
        # Run multiple connection cycles to get meaningful memory data
        num_cycles = 5
        queries_per_cycle = 3
        
        for cycle in range(num_cycles):
            print(f"  Cycle {cycle + 1}/{num_cycles}...")
            
            try:
                # Create connection
                conn = sql.connect(**connection_params)
                cursor = conn.cursor()
                
                # Execute several queries
                for query_num in range(queries_per_cycle):
                    try:
                        cursor.execute("SELECT 1 as test")
                        result = cursor.fetchall()
                        test_count += 1
                    except Exception as e:
                        print(f"    ⚠️  Query {query_num + 1} error: {e}")
                        error_count += 1
                
                # Close connection
                cursor.close()
                conn.close()
                
                # Check memory after each cycle
                current, peak = tracemalloc.get_traced_memory()
                print(f"    Memory: Current={format_bytes(current)}, Peak={format_bytes(peak)}")
                
            except Exception as e:
                print(f"    ⚠️  Connection error: {e}")
                error_count += 1
        
        # Force garbage collection
        gc.collect()
        
        # Get final memory snapshot
        snapshot = tracemalloc.take_snapshot()
        final_current, final_peak = tracemalloc.get_traced_memory()
        
        # Stop tracking
        tracemalloc.stop()
        
        # Get top allocations
        top_stats = snapshot.statistics('lineno')
        
        print(f"\n{'-'*80}")
        print(f"Memory Statistics: {mode}")
        print(f"{'-'*80}")
        print(f"Tests executed: {test_count}")
        print(f"Errors: {error_count}")
        print(f"\nMemory Usage:")
        print(f"  Initial Current: {format_bytes(initial_current)}")
        print(f"  Initial Peak:    {format_bytes(initial_peak)}")
        print(f"  Final Current:   {format_bytes(final_current)}")
        print(f"  Final Peak:      {format_bytes(final_peak)}")
        print(f"  Growth:          {format_bytes(final_current - initial_current)}")
        
        print(f"\nTop 15 Memory Allocations:")
        print(f"{'-'*80}")
        
        telemetry_related = []
        other_allocations = []
        
        for stat in top_stats[:30]:
            frame = stat.traceback[0]
            filename = frame.filename
            
            # Categorize allocations
            if 'telemetry' in filename.lower():
                telemetry_related.append(stat)
            elif len(other_allocations) < 15:
                other_allocations.append(stat)
        
        # Show top 15 overall
        for i, stat in enumerate(top_stats[:15], 1):
            frame = stat.traceback[0]
            filename = frame.filename.replace('/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/', '')
            marker = "🔴" if 'telemetry' in filename.lower() else "  "
            print(f"{marker} {i:2d}. {filename}:{frame.lineno}")
            print(f"      Size: {format_bytes(stat.size)}, Count: {stat.count:,}")
        
        if telemetry_related:
            print(f"\nTelemetry-Specific Allocations:")
            print(f"{'-'*80}")
            for i, stat in enumerate(telemetry_related[:10], 1):
                frame = stat.traceback[0]
                filename = frame.filename.replace('/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/', '')
                print(f"🔴 {i}. {filename}:{frame.lineno}")
                print(f"      Size: {format_bytes(stat.size)}, Count: {stat.count:,}")
        
        return {
            "mode": mode,
            "enable_telemetry": enable_telemetry,
            "force_enable": force_enable,
            "test_count": test_count,
            "error_count": error_count,
            "initial_current_bytes": initial_current,
            "initial_peak_bytes": initial_peak,
            "final_current_bytes": final_current,
            "final_peak_bytes": final_peak,
            "growth_bytes": final_current - initial_current,
            "telemetry_allocations_count": len(telemetry_related),
            "telemetry_allocations_size": sum(stat.size for stat in telemetry_related)
        }
        
    except Exception as e:
        print(f"\n❌ Error during test execution: {e}")
        import traceback
        traceback.print_exc()
        tracemalloc.stop()
        raise


def compare_results(result_off, result_on):
    """Compare memory results between telemetry OFF and ON."""
    
    print(f"\n{'='*80}")
    print("MEMORY FOOTPRINT COMPARISON")
    print(f"{'='*80}\n")
    
    print(f"{'Metric':<45} {'OFF':<18} {'ON':<18} {'Difference':<15}")
    print(f"{'-'*96}")
    
    # Peak memory
    peak_off = result_off['final_peak_bytes']
    peak_on = result_on['final_peak_bytes']
    peak_diff = peak_on - peak_off
    peak_diff_str = ("+" if peak_diff >= 0 else "") + format_bytes(peak_diff)
    print(f"{'Peak Memory Usage':<45} "
          f"{format_bytes(peak_off):>17} "
          f"{format_bytes(peak_on):>17} "
          f"{peak_diff_str:>15}")
    
    # Current memory
    current_off = result_off['final_current_bytes']
    current_on = result_on['final_current_bytes']
    current_diff = current_on - current_off
    current_diff_str = ("+" if current_diff >= 0 else "") + format_bytes(current_diff)
    print(f"{'Final Current Memory':<45} "
          f"{format_bytes(current_off):>17} "
          f"{format_bytes(current_on):>17} "
          f"{current_diff_str:>15}")
    
    # Growth during execution
    growth_off = result_off['growth_bytes']
    growth_on = result_on['growth_bytes']
    growth_diff = growth_on - growth_off
    growth_diff_str = ("+" if growth_diff >= 0 else "") + format_bytes(growth_diff)
    print(f"{'Memory Growth During Test':<45} "
          f"{format_bytes(growth_off):>17} "
          f"{format_bytes(growth_on):>17} "
          f"{growth_diff_str:>15}")
    
    # Telemetry-specific allocations
    if result_on['telemetry_allocations_count'] > 0:
        telemetry_size = result_on['telemetry_allocations_size']
        telemetry_size_str = "+" + format_bytes(telemetry_size)
        print(f"{'Telemetry-Specific Allocations':<45} "
              f"{'N/A':>17} "
              f"{format_bytes(telemetry_size):>17} "
              f"{telemetry_size_str:>15}")
    
    print(f"\n{'-'*96}")
    print("ANALYSIS:")
    print(f"{'-'*96}")
    
    # Calculate percentage overhead
    if peak_off > 0:
        percentage_increase = (peak_diff / peak_off) * 100
        print(f"\n📊 Telemetry peak memory overhead: {format_bytes(peak_diff)} ({percentage_increase:+.1f}%)")
    
    if current_off > 0:
        current_percentage = (current_diff / current_off) * 100
        print(f"📊 Telemetry current memory overhead: {format_bytes(current_diff)} ({current_percentage:+.1f}%)")
    
    # Provide assessment
    abs_peak_diff_mb = abs(peak_diff) / (1024**2)
    
    print(f"\n💡 Assessment:")
    if abs_peak_diff_mb < 5:
        print("   ✅ Telemetry has MINIMAL memory overhead (< 5 MB)")
    elif abs_peak_diff_mb < 20:
        print("   ⚠️  Telemetry has MODERATE memory overhead (5-20 MB)")
    elif abs_peak_diff_mb < 50:
        print("   🔶 Telemetry has NOTICEABLE memory overhead (20-50 MB)")
    else:
        print("   🚨 Telemetry has SIGNIFICANT memory overhead (> 50 MB)")
    
    print(f"\n{'='*80}\n")


def main():
    """Main function to run memory profiling comparison."""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║              TELEMETRY MEMORY FOOTPRINT ANALYSIS                             ║
║                                                                              ║
║  This script uses Python's tracemalloc to measure memory footprint           ║
║  of E2E operations with telemetry ENABLED vs DISABLED                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Run with telemetry OFF
        print("\n🔴 PHASE 1: Running with Telemetry DISABLED")
        result_off = run_telemetry_tests(enable_telemetry=False, force_enable=False)
        
        # Small pause between runs
        import time
        time.sleep(2)
        gc.collect()
        
        # Run with telemetry ON
        print("\n🟢 PHASE 2: Running with Telemetry ENABLED")
        result_on = run_telemetry_tests(enable_telemetry=True, force_enable=False)
        
        # Compare results
        compare_results(result_off, result_on)
        
        # Save results to JSON
        import json
        results_file = '/Users/nikhil.suri/Desktop/Databricks/Repos/peco/databricks-sql-python/memory_profile_results.json'
        with open(results_file, 'w') as f:
            json.dump({
                "telemetry_off": result_off,
                "telemetry_on": result_on,
                "comparison": {
                    "peak_diff_bytes": result_on['final_peak_bytes'] - result_off['final_peak_bytes'],
                    "current_diff_bytes": result_on['final_current_bytes'] - result_off['final_current_bytes'],
                    "telemetry_specific_bytes": result_on['telemetry_allocations_size']
                }
            }, f, indent=2)
        
        print(f"💾 Detailed results saved to: {results_file}")
        
        print(f"\n{'='*80}")
        print("✅ PROFILING COMPLETE")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n❌ Profiling failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
