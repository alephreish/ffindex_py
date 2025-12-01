import concurrent.futures
import subprocess
import sys, os
import argparse
import contextlib
from typing import TextIO
from collections.abc import Iterator

version = "1.0"
visit = "[https://github.com/alephreish/ffindex_py]"

class CustomHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog):
        super().__init__(prog, max_help_position = 28)

@contextlib.contextmanager
def open_file_or_stdout(filename: str | None, mode: str = 'w') -> TextIO:
    is_stdout = not filename
    fh = os.fdopen(sys.stdout.fileno(), mode) if is_stdout else open(filename, mode)
    try:
        yield fh
    finally:
        if not is_stdout:
            fh.close()

@contextlib.contextmanager
def open_file_or_dont(filename: str | None, mode: str = 'w') -> TextIO | None:
    dont_open = not filename
    fh = None if dont_open else open(filename, mode)
    try:
        yield fh
    finally:
        if not dont_open:
            fh.close()

def file_or_stream(filename: str | None) -> str | None:
    if not filename or filename == '-':
        return None
    else:
        return filename

def read_fasta(file: TextIO) -> tuple[str, str, str]:
    seq = name = header = ''
    for line in file:
        if line.startswith('>'):
            if seq:
                yield name, header, seq
            seq = ''
            header = line[1:].rstrip()
            name = header.split()[0]
        else:
            seq += line
    if seq:
        yield name, header, seq

def read_ffindex(file: TextIO) -> Iterator[tuple[str, str, int, int]]:
    for index, line in enumerate(file):
        name, start, length = line.split('\t')
        yield index, name, int(start), int(length)

def run_get() -> None:
    description = "ffindex_get re-implementation in python"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-n', action = 'store_true', help = 'Use index of entry instead of entry name.')
    arg_group.add_argument('--ignore-missing', action = 'store_true', help = 'Ignore missing entries (or indices).')
    arg_group.add_argument('--entries-file', metavar = 'FILE_WITH_ENTRIES', type = str, help = 'Text file with entries (or indices).')

    arg_group.add_argument('-d', metavar = 'DATA_FILENAME_OUT', type = str, help = 'FFindex data file where the results will be saved to.')
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'FFindex index file where the results will be saved to.')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')
    arg_group.add_argument('entries', metavar = 'entry name(s)', type = str, nargs = argparse.REMAINDER, help = 'Entry names (or indices).')

    args = parser.parse_args()
    use_index = args.n
    ffdata_in = args.data_filename
    ffindex_in = args.index_filename
    ffdata_out = file_or_stream(args.d)
    ffindex_out = args.i

    if ffdata_out and not ffindex_out or ffindex_out and not ffdata_out:
        raise ValueError("Either specify both -d and -i or none to output to stdout")
    if args.entries_file and args.entries:
        raise ValueError("Either specify in-line entries or a file with entries (or neither to fetch all entries)")
    if use_index and not args.entries_file and not args.entries:
        raise ValueError("For --use-index you need to provide in-line indexes or a file with entries (indexes)")

    entries = []
    if args.entries_file:
        with open(args.entries_file) as file:
            entries = [ line.rstrip() for line in file ]
        if not entries:
            raise ValueError("The provided entries file is empty")
    elif args.entries:
        entries = args.entries

    if len(entries) != len(set(entries)):
        raise ValueError("The list of the entries has duplicates")

    if use_index:
        for i, entry in enumerate(entries):
            try:
                entries[i] = int(entry)
            except Exception as e:
                raise ValueError(f"Integer expected for entry index, got '{entry}'")

    found = [ (None, None, None) ] * len(entries)

    has_entries = bool(entries)
    len_subtract = -1 if not ffdata_out else 0 # remove zero byte if output to stdout
    with open(ffindex_in, 'r') as index_in, open(ffdata_in, 'rb', buffering = 0) as data_in, open_file_or_stdout(ffdata_out, 'wb') as data_out:
        offset = 0
        for index, name, start, length in read_ffindex(index_in):
            if has_entries:
                needle = index if use_index else name
                if needle not in entries:
                    continue
                i = entries.index(needle)
                found[i] = name, offset, length
            else:
                found.append((name, offset, length))
            data_in.seek(start)
            record = data_in.read(length + len_subtract)
            data_out.write(record)
            offset += length
    if not args.ignore_missing:
        for i, (name, offset, length) in enumerate(found):
            if name is None:
                raise ValueError(f"Requested entry '{entries[i]}' not found in the index")
    if ffindex_out:
        with open(ffindex_out, 'w') as index_out:
            for name, offset, length in found:
                index_out.write(f"{name}\t{offset}\t{length}\n")

def read_header_line(file: TextIO) -> str:
    header = ''
    while True:
        data_chunk = file.read(1024)
        assert data_chunk, "Unexpected end of file - no null byte found"
        for byte in data_chunk:
            if byte < 33:
                return header
            else:
                header += chr(byte)

def run_rename() -> None:
    description = "rename ffindex records based on first line of data"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'FFindex index file where the results will be saved to.')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')

    args = parser.parse_args()
    ffdata_in = args.data_filename
    ffindex_in = args.index_filename
    ffindex_out = args.i

    names = {}
    with open(ffindex_in) as index_in, open(ffdata_in, 'rb') as data_in, open_file_or_stdout(ffindex_out, 'w') as index_out:
        for index, name, start, length in read_ffindex(index_in):
            data_in.seek(start)
            new_name = read_header_line(data_in)
            new_name = new_name.lstrip('#>')
            assert new_name, f"Empty name at index {index}"
            assert new_name not in names, f"Duplicate name '{new_name}'"
            names[new_name] = True
            index_out.write(f'{new_name}\t{start}\t{length}\n')

def apply_to_record(command: list[str], name: str, start: int, length: int, ffdata_in: str, on_error_original: bool) -> tuple[str, bytes, bytes, int]:
    """Run a command on a single record
    """
    with open(ffdata_in, 'rb') as ffdata:
        ffdata.seek(start)
        record = ffdata.read(length - 1)
        process = subprocess.Popen(command, stdout = subprocess.PIPE, stdin = subprocess.PIPE, stderr = subprocess.PIPE)
        data_out, data_err = process.communicate(input = record)
        if process.returncode > 0:
            data_out = record if on_error_original else b''
        return name, data_out, data_err, process.returncode

def run_apply() -> None:
    description = "ffindex_apply re-implementation in python"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default = argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-j', metavar = "JOBS", type = int, default = 1, help = 'Number of parallel jobs.')
    arg_group.add_argument('-q', action = 'store_true', help = 'Silence the logging of every processed entry.')
    arg_group.add_argument('-k', action = 'store_true', help = 'Keep unmerged ffindex splits (not implemented).')
    arg_group.add_argument('-d', metavar = 'DATA_FILENAME_OUT', type = str, help = 'FFindex data file where the results will be saved to.')
    arg_group.add_argument('-i', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'FFindex index file where the results will be saved to.')

    index_order_default = 'keep'
    on_error_default = 'exit'
    arg_group.add_argument('--index-order', type = str, choices = [ 'keep', 'sort', 'data' ], default = index_order_default, help = f'How to order ffindex records [{index_order_default}] (NB: ffindex_apply keeps, ffindex_apply_mpi sorts).')
    arg_group.add_argument('--on-error', type = str, choices = [ 'exit', 'ignore', 'blank', 'original' ], default = on_error_default, help = f'Action on exit code >0 [{on_error_default}] (NB: ffindex_apply ignores).')

    arg_group.add_argument('data_filename', metavar = 'DATA_FILENAME', type = str, help = 'Input ffindex data file.')
    arg_group.add_argument('index_filename', metavar = 'INDEX_FILENAME', type = str, help = 'Input ffindex index file.')
    command_meta_var = 'PROGRAM [PROGRAM_ARGS]*'
    arg_group.add_argument('command', metavar = command_meta_var, type = str, nargs = '+', help = 'Program to be executed for every ffindex entry.')

    usage_str = parser.format_usage() # get the generated usage string

    # make changes to the usage_str as desired
    # usage_str = usage_str.replace("usage: ", "")
    usage_str = usage_str.replace(f"{command_meta_var} [{command_meta_var} ...]", f"-- {command_meta_var}")
    parser.usage = usage_str

    args = parser.parse_args()

    ffdata_in, ffindex_in = args.data_filename, args.index_filename
    ffdata_out = file_or_stream(args.d)
    ffindex_out = args.i

    if ffdata_out and not ffindex_out or ffindex_out and not ffdata_out:
        raise ValueError("Either specify both -d and -i or none to output to stdout")

    cmd = args.command
    jobs = args.j
    verbose = not args.q
    index_order_sort = args.index_order == 'sort'
    index_order_keep = args.index_order == 'keep'
    index_order_not_data = args.index_order != 'data'
    on_error_ignore = args.on_error == 'ignore'
    on_error_blank = args.on_error == 'blank'
    on_error_original = args.on_error == 'original'
    on_error_exit = args.on_error == 'exit'

    if index_order_keep and on_error_ignore:
        raise ValueError("--index-order keep is incompatible with --on-error ignore")

    is_stdout = not ffdata_out
    # parallel execution with ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers = jobs) as executor:
        futures = []
        ffindex = {}
        names = []
        with open(ffindex_in, 'r') as f:
            for index, name, start, length in read_ffindex(f):
                ffindex[name] = start, length
                names.append(name)
        if index_order_sort:
            names.sort()
        for name in names:
            start, length = ffindex[name]
            futures.append(executor.submit(apply_to_record, cmd, name, start, length, ffdata_in, on_error_original))

        # Write the output
        with open_file_or_stdout(ffdata_out, 'wb') as outdata, open_file_or_dont(ffindex_out, 'w') as outindex:
            offset = 0
            index_buf = {}
            for future in concurrent.futures.as_completed(futures):
                name, data_out, data_err, returncode = future.result()
                if returncode > 0:
                    msg = f'Got exit code {returncode} for record {name} with stderr content: {data_err.decode("utf-8")}'
                    if on_error_exit:
                        raise ValueError(msg)
                    else:
                        print(msg, file = sys.stderr)
                    if on_error_ignore:
                        continue
                if verbose:
                    print(name)
                if is_stdout:
                    outdata.write(data_out)
                else:
                    outdata.write(data_out + b'\0')
                    length = len(data_out) + 1
                    if index_order_not_data:
                        index_buf[name] = offset, length
                        while names and names[0] in index_buf:
                            name0 = names.pop(0)
                            offset0, length0 = index_buf.pop(name0)
                            outindex.write(f"{name0}\t{offset0}\t{length0}\n")
                    else:
                        outindex.write(f"{name}\t{offset}\t{length}\n")
                    offset += length
            assert not index_buf, "index_buf is not empty, output index is corrupt"

def run_reindex() -> None:
    description = "Re-index an existing .ffdata file"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default=argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('-p', action = 'store_true', help = 'Parse names from the first line in each record.')
    arg_group.add_argument('-r', action = 'store_true', help = 'Rename duplicate names (otherwise raise exception on duplicate).')

    arg_group.add_argument('ffdata', metavar = 'DATA_FILENAME_IN', type = str, help = 'Path to the ffdata file to be reindexed')
    arg_group.add_argument('ffindex', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'Path to the output ffindex file')

    args = parser.parse_args()

    ffdata_file = args.ffdata
    ffindex_file = args.ffindex
    parse_names = args.p
    allow_duplicates = args.r

    with open(ffdata_file, 'rb') as ffdata_file, open(ffindex_file, 'w') as ffindex_file:
        chunk_size = 1024*1024  # chunk size of 1MB
        offset = 0
        record_length = 1
        index = 0
        headers = {}
        in_header = True
        header = ''
        while True:
            chunk = ffdata_file.read(chunk_size)
            if chunk: # if data exists in chunk
                for byte in chunk:
                    if byte == 0: # if null character is found
                        if parse_names:
                            header= header.lstrip('#>')
                            assert header, f"Empty name at index {index}"
                            while header in headers:
                                assert allow_duplicates, f"Duplicate name '{header}'"
                                header += '^'
                            headers[header] = True
                            name = header
                        else:
                            name = index
                        ffindex_file.write(f'{name}\t{offset}\t{record_length}\n')
                        offset += record_length
                        record_length = 1
                        index += 1
                        header = ''
                        in_header = True
                    else:
                        if byte < 33:
                            in_header = False
                        if parse_names and in_header:
                            header += chr(byte)
                        record_length += 1
            else:
                break

def run_from_fasta() -> None:
    description = "Create a ffindex database from a fasta file"
    parser = argparse.ArgumentParser(description = f"{description}\n{visit}", add_help = False, formatter_class = CustomHelpFormatter)

    arg_group = parser.add_argument_group()

    arg_group.add_argument('-h', '--help', action = 'help', default=argparse.SUPPRESS, help = "Show this help message and exit.")
    arg_group.add_argument('-v', '--version', action = 'version', version = "%(prog)s v{version}", help = "Show program's version number and exit.")
    arg_group.add_argument('ffdata', metavar = 'DATA_FILENAME_OUT', type = str, help = 'Path to output ffdata file.')
    arg_group.add_argument('ffindex', metavar = 'INDEX_FILENAME_OUT', type = str, help = 'Path to output ffindex file.')
    arg_group.add_argument('fasta', metavar = 'FASTA', type = str, help = 'Path to input fasta file.')

    args = parser.parse_args()

    fasta_file = args.fasta
    ffdata_file, ffindex_file = args.ffdata, args.ffindex

    with open(fasta_file, 'r') as fasta, open(ffdata_file, 'wb') as ffdata, open(ffindex_file, 'w') as ffindex:
        offset = 0
        for name, header, seq in read_fasta(fasta):
            fasta_record_bytes = f'>{header}\n{seq}\0'.encode('utf-8')
            record_length = len(fasta_record_bytes)
            ffdata.write(fasta_record_bytes)
            ffindex.write(f'{name}\t{offset}\t{record_length}\n')
            offset += record_length
