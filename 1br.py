import sys
from typing import Dict, List, Tuple
import multiprocessing as mp
import time

def profile_exec(f):
    def wrapped_f(*args, **kwargs):
        pc_begin , pt_begin = time.perf_counter(), time.process_time()
        res = f(*args, **kwargs)
        pc_end, pt_end = time.perf_counter(), time.process_time()

        print (f"executed {f.__name__}")
        print (f"CPU time: {pc_end - pc_begin:.2f} seconds")
        print (f"Real time: {pt_end - pt_begin:.2f} seconds")

        return res

    return wrapped_f


@profile_exec
def get_num_lines(file_name: str) -> List[str]:
    count = 0
    with open(file_name, 'rb') as file:
        count = sum(1 for _ in file)
        
    return count


def process_chunk(file_name: str, begin: int, end: int) -> Dict[str, Tuple[float]]:
    print (f'processing chunk {begin}:{end}')
    pc_begin, pt_begin = time.perf_counter(), time.process_time()
    res = {}
    with open(file_name, 'r') as file:
        for i, line in enumerate(file):
            if i >= begin and i < end:
                name, val = line.split(';')
                if res.get(name) is None:
                    res[name] = (float('inf'), -float('inf'), 0.0, 0.0)
                
                val = float(val)
                # update stats for this location
                min_val, max_val, csum, count = res[name]
                res[name] = (min(val, min_val), max(val, max_val), csum + val, count + 1)
            elif i >= end:
                break
    
    pc_end, pt_end = time.perf_counter(), time.process_time()
    print (f"CPU time: {pc_end - pc_begin:.2f} seconds, Real time: {pt_end - pt_begin:.2f} seconds, chunk : {begin}:{end}")
    
    return res

@profile_exec
def process_chunks_parallel(chunks: List[Tuple[int, int]], num_cpus: int):
    chunk_results = None
    with mp.Pool(num_cpus) as pool:
        chunk_results = pool.starmap(process_chunk, chunks)
    
    return chunk_results
    

@profile_exec
def get_combined_results(chunk_results) -> Dict[str, Tuple[float, float, float, float]]:
    res = {}
    for chunk_result in chunk_results:
        for name, val in chunk_result.items():
            if res.get(name) is None:
                res[name] = (float('inf'), -float('inf'), 0.0, 0.0)
            
            prev_min, prev_max, prev_csum, prev_count = res[name]
            cur_min, cur_max, cur_csum, cur_count = val
            res[name] = ( min(prev_min, cur_min), max(prev_max, cur_max), prev_csum + cur_csum, prev_count + cur_count )
    return res

@profile_exec
def get_sorted_results(res: Dict[str, Tuple[float, float, float, float]]) -> Dict[str, Tuple[float, float, float, float]]:
    pr_res = {}
    for name, values in sorted(res.items()):
        min_val, max_val, csum, count = values
        pr_res[name] = f'{min_val:.1f}/{csum / count:.1f}/{max_val:.1f}'
    
    return pr_res

@profile_exec
def process_file(file_name: str) -> None:
    num_lines = get_num_lines(file_name)
    num_cpus = mp.cpu_count()
    chunk_size = num_lines // num_cpus
    
    print (f'num_lines : {num_lines}, number of cpus for multi-processing : {num_cpus}, chunk_size : {chunk_size}')

    # process file in chunks
    chunks = [ (file_name, i * chunk_size, (i + 1) * chunk_size) if i != num_cpus - 1 else (file_name, i * chunk_size, num_lines) for i in range(num_cpus)]
    chunk_results = process_chunks_parallel(chunks, num_cpus)

    # combine results from each chunk
    res  = get_combined_results(chunk_results)
    
    # sort by key and add min, max, avg to results
    return get_sorted_results(res)

if __name__ == "__main__":
    input_file = sys.argv[1]
    print (process_file(input_file))
