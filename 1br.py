import profile
import os
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

@profile_exec
def get_chunk_positions(file_name: str) -> List[Tuple[str, int, int]]:
    size = os.path.getsize(file_name)
    num_cpus = mp.cpu_count()
    chunk_size = size // num_cpus

    chunks = []
    with open(file_name, "rb") as file:
        cbegin, cend = 0, 0
        while cbegin < size:
            cend = min(size, cbegin + chunk_size)

            def is_new_line(pos: int) -> True:
                if pos == 0:
                    return True
                else:
                    file.seek(pos - 1)
                    return file.read(1) == b"\n"
            
            while not is_new_line(cend):
                cend -= 1
            
            if cbegin == cend:
                file.seek(cend)
                file.readline()
                cend = file.tell()
            
            chunks.append((file_name, cbegin, cend))
            cbegin = cend
    
    return chunks


def process_chunk2(file_name: str, begin: int, end: int) -> Dict[str, Tuple[float]]:
    print (f'processing chunk {begin}:{end}')
    pc_begin, pt_begin = time.perf_counter(), time.process_time()
    res = {}
    with open(file_name, 'r') as file:
        file.seek(begin)
        for line in file:
            if begin > end:
                break

            name, val = line.split(';')
            if res.get(name) is None:
                res[name] = (float('inf'), -float('inf'), 0.0, 0.0)
            
            val = float(val)
            # update stats for this location
            min_val, max_val, csum, count = res[name]
            res[name] = (min(val, min_val), max(val, max_val), csum + val, count + 1)
            begin += len(line)

    pc_end, pt_end = time.perf_counter(), time.process_time()
    print (f"CPU time: {pc_end - pc_begin:.2f} seconds, Real time: {pt_end - pt_begin:.2f} seconds, chunk : {begin}:{end}")
    
    return res
    
@profile_exec
def process_chunks_parallel(chunks: List[Tuple[int, int]], num_cpus: int):
    chunk_results = None
    with mp.Pool(num_cpus) as pool:
        chunk_results = pool.starmap(process_chunk2, chunks)
    
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
    # process file in chunks
    chunks = get_chunk_positions(file_name)
    chunk_results = process_chunks_parallel(chunks, mp.cpu_count())

    # combine results from each chunk
    res  = get_combined_results(chunk_results)
    
    # sort by key and add min, max and avg to results
    return get_sorted_results(res)

if __name__ == "__main__":
    input_file = sys.argv[1]
    print (process_file(input_file))
