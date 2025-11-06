ffindex\_py
==========

[ffindex](https://github.com/ahcm/ffindex) re-implementation in python.

Currently implemented are:

* `ffindex_apply_py` -- a parallel version of `ffindex_apply` without restriction on the record name length and dependence on MPI
* `ffindex_from_fasta_py` -- a version of `ffindex_from_fasta` without restriction on the record name length
* `ffindex_get_py` -- get entries from an ffindex database
* `ffindex_reindex_py` -- re-index an existing `.ffdata` file
* `ffindex_rename_py` -- rename entries `.ffindex` from first word of the record in `.ffdata`
