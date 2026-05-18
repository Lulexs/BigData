from functools import wraps
import argparse
import pandas as pd
import time

FILEPATH = './doc.csv'


def mode_1(args):
    
    start_time = time.perf_counter()
    df = pd.read_csv(FILEPATH)
    print(df.dtypes)
    end_time_io = time.perf_counter()
    rows_read = len(df)

    if args.filter is not None:
        for filter_expr in args.filter.split(","):
            if "=" in filter_expr:
                col, val = filter_expr.split("=", 1)
                col = col.strip()
                val = val.strip()

                df = df[
                    df[col].astype(str).str.lower() == val.lower()
                ]

            elif ">" in filter_expr:
                col, val = filter_expr.split(">", 1)
                col = col.strip()
                val = float(val.strip())

                df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df[df[col] > val]

            elif "<" in filter_expr:
                col, val = filter_expr.split("<", 1)
                col = col.strip()
                val = float(val.strip())

                df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df[df[col] < val]

            else:
                print(f"WARN: Unrecognized filter {filter_expr}")
    end_time_filtering = time.perf_counter()
    rows_remaining = len(df)

    if args.sort is not None:
        sorts = []
        ascs = []
        for sort in args.sort.split(","):
            sort, asc = sort.split(".")
            sorts.append(sort)
            ascs.append(asc == '1')

        df = df.sort_values(by=sorts, ascending=ascs)

    end_time_sort = time.perf_counter()

    if args.count is not None:
        count_df = pd.DataFrame({
            "count": [len(df)]
        })
        count_df.to_csv(args.output, index=False)
    else:
        df.to_csv(args.output, index=False)
    
    end_time_write = time.perf_counter()

    total_time = end_time_write - start_time
    io_time = end_time_io - start_time
    filtering_time = end_time_filtering - end_time_io
    sorting_time = end_time_sort - end_time_filtering
    write_time = end_time_write - end_time_sort
    total_processing_time = end_time_sort - end_time_io

    print(
        "\n"
        "Execution summary\n"
        "-----------------\n"
        f"Input read time:   {io_time:.4f} seconds\n"
        f"Total rows read:   {rows_read}\n"
        f"Filtering time:    {filtering_time:.4f} seconds\n"
        f"Rows filtered:     {rows_read - rows_remaining}\n" 
        f"Sorting time:      {sorting_time:.4f} seconds\n"
        f"Processing time:   {total_processing_time:.4f} seconds\n"
        f"Output write time: {write_time:.4f} seconds\n"
        f"Rows written:      {rows_remaining}\n"
        f"Total time:        {total_time:.4f} seconds\n"
    )    


def mode_2(args):
    start_time = time.perf_counter()

    df = pd.read_csv(FILEPATH)
    end_time_io = time.perf_counter()
    rows_read = len(df)

    if args.output is None:
        print("ERROR: --output is required for mode 2")
        return

    if args.agg is None:
        print("ERROR: --agg is required for mode 2")
        return

    if args.groupby is None:
        print("ERROR: --groupby is required for mode 2")
        return

    agg_cols = [col.strip() for col in args.agg.split(",")]
    groupby_cols = [col.strip() for col in args.groupby.split(",")]

    missing_cols = [col for col in agg_cols + groupby_cols if col not in df.columns]

    if missing_cols:
        print(f"ERROR: Column(s) not found: {', '.join(missing_cols)}")
        return

    overlap = set(agg_cols) & set(groupby_cols)
    if overlap:
        print("ERROR: Column(s) cannot be used both for aggregation and grouping: " + ", ".join(overlap))
        return

    for col in agg_cols:
        converted = pd.to_numeric(df[col], errors="coerce")

        if converted.notna().sum() == 0:
            print(f"ERROR: --agg column '{col}' must be numeric")
            return

        non_null_original = df[col].notna().sum()
        valid_numeric = converted.notna().sum()

        if valid_numeric < non_null_original:
            print(f"ERROR: --agg column '{col}' contains non-numeric values and cannot be fully aggregated")
            return

        df[col] = converted

    end_time_validation = time.perf_counter()

    result = df.groupby(groupby_cols)[agg_cols].agg([
        "count",
        "min",
        "max",
        "mean",
        "median",
        "std",
        "var",
        "sum"
    ])

    result.columns = [f"{col}_{stat}" for col, stat in result.columns]

    result = result.reset_index()

    end_time_aggregation = time.perf_counter()

    result.to_csv(args.output, index=False)

    end_time_write = time.perf_counter()

    total_time = end_time_write - start_time
    io_time = end_time_io - start_time
    validation_time = end_time_validation - end_time_io
    aggregation_time = end_time_aggregation - end_time_validation
    write_time = end_time_write - end_time_aggregation

    print(
        "\n"
        "Execution summary\n"
        "-----------------\n"
        f"Input read time:      {io_time:.4f} seconds\n"
        f"Total rows read:      {rows_read}\n"
        f"Validation time:      {validation_time:.4f} seconds\n"
        f"Aggregation time:     {aggregation_time:.4f} seconds\n"
        f"Output write time:    {write_time:.4f} seconds\n"
        f"Groups written:       {len(result)}\n"
        f"Total time:           {total_time:.4f} seconds\n"
    )
    
def main(args):
    start_time = time.perf_counter()
    if args.mode == '1':
        mode_1(args)
    elif args.mode == '2':
        mode_2(args)
    else:
        print("ERROR: Unsupported mode " + args.mode)

    end_time = time.perf_counter()
    print(f"Elapsed time for mode {args.mode}: {end_time - start_time}s")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode')
    parser.add_argument('--output')
    
    # mode1 args
    parser.add_argument('--filter')
    parser.add_argument('--sort')
    parser.add_argument('--count')

    # mode2 args
    parser.add_argument('--agg')
    parser.add_argument('--groupby')

    args = parser.parse_args()
    main(args)