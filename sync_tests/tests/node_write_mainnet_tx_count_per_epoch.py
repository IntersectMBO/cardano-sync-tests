import pandas as pd

import sync_tests.utils.aws_db as aws_db_utils
import sync_tests.utils.blockfrost as blockfrost_utils
import sync_tests.utils.helpers as utils


def update_mainnet_tx_count_per_epoch() -> None:
    _env, table_name = "mainnet", "mainnet_tx_count"
    current_epoch_no = blockfrost_utils.get_current_epoch_no()
    assert current_epoch_no is not None  # TODO: refactor
    print(f"current_epoch_no   : {current_epoch_no}")

    max_epoch = aws_db_utils.get_max_epoch(table_name)
    assert max_epoch is not None  # TODO: refactor

    last_added_epoch_no = int(max_epoch)
    print(f"last_added_epoch_no: {last_added_epoch_no}")

    df_column_names = ["epoch_no", "tx_count"]
    df = pd.DataFrame(columns=df_column_names)

    if current_epoch_no > last_added_epoch_no + 1:
        # adding values into the db only for missing full epochs
        # (ignoring the current/incomplete epoch)
        for epoch_no in range(last_added_epoch_no + 1, current_epoch_no):
            utils.print_message(type="info", message=f"Getting values for epoch {epoch_no}")
            tx_count = blockfrost_utils.get_tx_count_per_epoch(epoch_no)
            print(f"  - tx_count: {tx_count}")
            new_row_data = {"epoch_no": epoch_no, "tx_count": tx_count}
            new_row = pd.DataFrame([new_row_data])
            df = pd.concat([df, new_row], ignore_index=True)

        col_to_insert = list(df.columns)
        val_to_insert = df.values.tolist()
        if not aws_db_utils.insert_values_into_db(table_name, col_to_insert, val_to_insert, True):
            print(f"col_to_insert: {col_to_insert}")
            print(f"val_to_insert: {val_to_insert}")
    else:
        utils.print_message(type="warn", message="There are no new finalized epochs to be added")


if __name__ == "__main__":
    update_mainnet_tx_count_per_epoch()
