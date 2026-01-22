[![CI](https://github.com/derekcnursey/college_basketball/actions/workflows/ci.yml/badge.svg)](https://github.com/derekcnursey/college_basketball/actions/workflows/ci.yml)

Note: In some sandboxed environments (including Codex), Torch/OpenMP can abort with "OMP: Error #179" when running the backfill. Run the backfill from a local terminal instead. If you still see the error locally, set:
`KMP_SHM_DISABLE=1 KMP_INIT_AT_FORK=FALSE OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1`.
