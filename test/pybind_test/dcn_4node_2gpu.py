"""
 Copyright (c) 2023, NVIDIA CORPORATION.
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

import hugectr
from mpi4py import MPI
import threading
import sys


def model_test(json_file):
    solver = hugectr.CreateSolver(
        max_eval_batches=100,
        batchsize_eval=16384,
        batchsize=16384,
        vvgpu=[[0, 1], [2, 3], [4, 5], [6, 7]],
        i64_input_key=True,
        use_mixed_precision=False,
        repeat_dataset=True,
        use_cuda_graph=True,
    )
    reader = hugectr.DataReaderParams(
        data_reader_type=hugectr.DataReaderType_t.Parquet,
        source=["./_file_list.txt"],
        eval_source="./_file_list.txt",
        check_type=hugectr.Check_t.Non,
        slot_size_array=[
            381808,
            22456,
            14763,
            7118,
            19308,
            4,
            6443,
            1259,
            54,
            341642,
            112151,
            94957,
            11,
            2188,
            8399,
            61,
            4,
            949,
            15,
            382633,
            246818,
            370704,
            92823,
            9773,
            78,
            34,
        ],
    )
    optimizer = hugectr.CreateOptimizer(optimizer_type=hugectr.Optimizer_t.Adam)
    model = hugectr.Model(solver, reader, optimizer)
    model.construct_from_json(graph_config_file=json_file, include_dense_network=True)
    model.summary()
    model.compile()
    model.fit(
        max_iter=10000,
        display=200,
        eval_interval=1000,
        snapshot=100000,
        snapshot_prefix="dcn",
    )


if __name__ == "__main__":
    json_file = sys.argv[1]
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    thread = threading.Thread(target=model_test, args=(json_file,), name="[rank-%d train]" % rank)
    current_thread = threading.currentThread()
    print("[HUGECTR][INFO] %s is main thread: %s" % (current_thread.name, MPI.Is_thread_main()))
    print("[HUGECTR][INFO] before: rank %d " % (rank))
    # start the thread
    thread.start()
    # wait for terminate
    thread.join()
    print("[HUGECTR][INFO] after: rank %d " % (rank))
