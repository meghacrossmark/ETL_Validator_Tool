def dataCompare(source_df, target_df, primary_key_source, primary_key_target=0):
    df1 = source_df
    df2 = target_df

    #add code to sort dfs in case of 'all' on first column

    kwargs = {'on_index': True} if "all" in primary_key_source else {'join_columns': primary_key_source}
    comparison = dc.Compare(df1, df2, **kwargs)

    comparison.report(10, 10, "dcRep.html")

    print("=====================================================================================================")

    sample_mismatches = []
    for col in comparison.intersect_columns():
        try:
            mismatch_df = comparison.sample_mismatch(column=col, sample_count=5)
            if not mismatch_df.empty:
                sample_mismatches.extend(mismatch_df.to_dict(orient="records"))
        except Exception:
            continue

    result = {
        "summary": {
            "df1_shape": df1.shape,
            "df2_shape": df2.shape,
            "columns_compared": len(comparison.intersect_columns()),
            "matched_rows": comparison.matches(),
            "mismatched_rows": comparison.all_mismatch().shape[0],
            "rows_only_in_source": comparison.df1_unq_rows.shape[0],
            "rows_only_in_target": comparison.df2_unq_rows.shape[0],
            "match_percentage": round(comparison.count_matching_rows() / comparison.df1.shape[0] * 100, 2) if df1.shape[
                                                                                                                  0] > 0 else 0
        },
        "details": {
            "columns_in_common": comparison.intersect_columns(),
            "columns_in_df1_only": comparison.df1_unq_columns(),
            "columns_in_df2_only": comparison.df2_unq_columns(),
            "sample_mismatches": sample_mismatches,
            "rows_only_in_source_sample": comparison.df1_unq_rows.head(5).to_dict(orient="records"),
            "rows_only_in_target_sample": comparison.df2_unq_rows.head(5).to_dict(orient="records"),
        }
    }

    print("\n\nFULL DETAILS OF DIFFERING ROWS")
    print("==============================\n")

    if not comparison.df1_unq_rows.empty:
        print("Complete Rows Only in First DataFrame:")
        print("------------------------------------")

        if 'join_columns' in kwargs:
            join_keys_df1_only = comparison.df1_unq_rows[kwargs['join_columns'].lower()].values

            mask = df1[kwargs['join_columns'].lower()].isin([x[0] for x in join_keys_df1_only])
            if len(kwargs['join_columns']) > 1:
                for i, col in enumerate(kwargs['join_columns'][1:], 1):
                    mask = mask & df1[col].isin([x[i] for x in join_keys_df1_only])

            df1_only_full = df1[mask]
            print(df1_only_full)
        else:
            indices_df1_only = comparison.df1_unq_rows.index
            df1_only_full = df1.loc[indices_df1_only]
            print(df1_only_full)

    if not comparison.df2_unq_rows.empty:
        print("\nComplete Rows Only in Second DataFrame:")
        print("-------------------------------------")

        if 'join_columns' in kwargs:
            join_keys_df2_only = comparison.df2_unq_rows[kwargs['join_columns']].values

            mask = df2[kwargs['join_columns'][0]].isin([x[0] for x in join_keys_df2_only])
            if len(kwargs['join_columns']) > 1:
                for i, col in enumerate(kwargs['join_columns'][1:], 1):
                    mask = mask & df2[col].isin([x[i] for x in join_keys_df2_only])

            df2_only_full = df2[mask]
            print(df2_only_full)
        else:

            indices_df2_only = comparison.df2_unq_rows.index
            df2_only_full = df2.loc[indices_df2_only]
            print(df2_only_full)

    mismatches = comparison.all_mismatch()

    if not mismatches.empty:
        print("\nRows with Differing Values:")
        print("-------------------------")

        if 'join_columns' in kwargs:
            join_keys = kwargs['join_columns']

            valid_keys = [col for col in join_keys if col in mismatches.columns]
            unique_keys = mismatches[valid_keys].drop_duplicates()

            df1_diff_rows = pd.DataFrame()
            df2_diff_rows = pd.DataFrame()

            for _, key_values in unique_keys.iterrows():
                mask1 = df1[join_keys[0]] == key_values[join_keys[0]]
                mask2 = df2[join_keys[0]] == key_values[join_keys[0]]

                for col in join_keys[1:]:
                    mask1 &= df1[col] == key_values[col]
                    mask2 &= df2[col] == key_values[col]

                df1_diff_rows = pd.concat([df1_diff_rows, df1[mask1]])
                df2_diff_rows = pd.concat([df2_diff_rows, df2[mask2]])

            print("DataFrame 1 Rows:")
            print(df1_diff_rows)
            print("\nDataFrame 2 Rows:")
            print(df2_diff_rows)

            # Show detailed column-level differences
            for _, key_row in unique_keys.iterrows():
                print(f"\nDifferences for row with keys:")
                print(" | ".join([f"{col} = {key_row[col]}" for col in join_keys]))
                row1 = df1
                row2 = df2
                for col in join_keys:
                    row1 = row1[row1[col] == key_row[col]]
                    row2 = row2[row2[col] == key_row[col]]

                if not row1.empty and not row2.empty:
                    row1 = row1.iloc[0]
                    row2 = row2.iloc[0]
                    for col in df1.columns:
                        if col in df2.columns and row1[col] != row2[col]:
                            print(f"  {col}: {row1[col]} (df1) vs {row2[col]} (df2)")

        else:
            indices_diff = mismatches.index.unique()
            df1_diff = df1.loc[indices_diff]
            df2_diff = df2.loc[indices_diff]
            print("DataFrame 1 Rows:")
            print(df1_diff)
            print("\nDataFrame 2 Rows:")
            print(df2_diff)

    print(comparison)

    #code to get values from indices for data comparision v1

    source_data = self.source_df
    target_data = self.target_df

    query = f"SELECT * FROM source_data WHERE {primary_key_source} IN ("
    query += ", ".join([f"'{ind}'" if isinstance(ind, str) else str(ind) for ind in df1_unique_indexes])
    query += ")"

    newDf = ps.sqldf(query, locals())
    print(newDf)

    query = f"SELECT * FROM target_data WHERE {primary_key_source} IN ("
    query += ", ".join([f"'{ind}'" if isinstance(ind, str) else str(ind) for ind in df2_unique_indexes])
    query += ")"

    newDf = ps.sqldf(query, locals())
    print(newDf)

    # Query and print mismatched rows side by side
    if not partial_mismatch_indexes.empty:
        query_mismatch_df1 = f"SELECT * FROM source_data WHERE {primary_key_source} IN ("
        query_mismatch_df1 += ", ".join(
            [f"'{ind}'" if isinstance(ind, str) else str(ind) for ind in partial_mismatch_indexes])
        query_mismatch_df1 += ")"
        df1_mismatch = ps.sqldf(query_mismatch_df1, locals())

        query_mismatch_df2 = f"SELECT * FROM target_data WHERE {primary_key_source} IN ("
        query_mismatch_df2 += ", ".join(
            [f"'{ind}'" if isinstance(ind, str) else str(ind) for ind in partial_mismatch_indexes])
        query_mismatch_df2 += ")"
        df2_mismatch = ps.sqldf(query_mismatch_df2, locals())

        merged_mismatch = pd.merge(
            df1_mismatch,
            df2_mismatch,
            on=primary_key_source,
            how='outer',
            suffixes=('_source', '_dest')
        )

        print("\nMismatched rows (side by side):")
        print(merged_mismatch)

    else:
        print("\nNo mismatched rows found.")
