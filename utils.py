import logging
from pathlib import Path
import pandas as pd
from typing import List

def get_app_root():
    return Path(__file__).parent

def get_revenue_per_night_per_reservation(list_revenue):
    df_revenues = pd.DataFrame()

    for rev in list_revenue:
        amount = rev['amount']
        del rev['amount']
        df_one_rev = pd.DataFrame(rev, index=[0])
        df_amount = pd.DataFrame(amount, index=[0])
        df_one_rev = pd.concat([df_one_rev, df_amount], axis=1)
        df_revenues = pd.concat([df_revenues, df_one_rev])

    return df_revenues.rename(columns={"id": "revenue_reservation_id"})

def is_a_sublist(lst1:List, lst2:List):
    """
    check if lst1 is a sublist of lst2
    """
    return all(i in lst2 for i in lst1)

def convert_datetime_to_date(df:pd.DataFrame, col_to_convert='startDate',
                             new_col_name = 'consumed_date'):
    df[new_col_name] = pd.to_datetime(df[col_to_convert]).dt.date

def process_availability_space(file_path:str):
    """
    process the raw data of the space availability

    """
    df_space_availability_raw = pd.read_json(file_path)
    convert_datetime_to_date(df_space_availability_raw, col_to_convert='date')
    df_space_availability_grp = df_space_availability_raw.groupby(['consumed_date']).agg(
        rooms_available=('availability', 'sum')).reset_index()

    df_space_availability_grp.loc[:, 'consumed_date'] = pd.to_datetime(
        df_space_availability_grp.consumed_date)
    return df_space_availability_grp

def process_reservations_data(file_path:str):
    df_reservations_raw = pd.read_json(file_path)

    df_flat_reservation = pd.DataFrame()
    for _, row in df_reservations_raw.iterrows():
        df_row = row.to_frame().T
        df_revenues_for_resev = get_revenue_per_night_per_reservation(row['revenues'])
        # assert test to check if the 'spaceReservationId' in 'revenue' key
        # is the same as 'id' used in the reservation row
        lst_ids_revenues_reservation = list(df_revenues_for_resev['spaceReservationId'].unique())
        lst_ids_row_reservation = list(df_row['id'])
        assert is_a_sublist(lst_ids_revenues_reservation, lst_ids_row_reservation)
        df_reservations = pd.merge(df_row, df_revenues_for_resev, how='right',
                                   left_on=['id'], right_on='spaceReservationId')
        df_flat_reservation = pd.concat([df_flat_reservation,df_reservations])
    convert_datetime_to_date(df_flat_reservation)
    df_flat_reservation.loc[:, 'consumed_date'] = pd.to_datetime(
        df_flat_reservation.consumed_date)

    return df_flat_reservation.reset_index()

test_methods = False

if test_methods is True:
    file_path_reservation = '/Users/houssemeddinedridi/PycharmProjects/productX-calculation/data/reservations-s3'
    file_path_space_availability = '/Users/houssemeddinedridi/PycharmProjects/productX-calculation/data/space-availability-s3'
    df_reservations = process_reservations_data(file_path_reservation)
    df_space_availability = process_availability_space(file_path_space_availability)

    df_flat_reservation_availability = df_reservations.merge(df_space_availability, how='left', on='consumed_date')
    print(df_flat_reservation_availability.info())
    # df_flat_reservation_availability.loc[:, 'consumed_date'] = pd.to_datetime(df_flat_reservation_availability.consumed_date)
    df_monthly = df_flat_reservation_availability[['consumed_date', 'netValue', 'grossValue'
                                       ]].set_index(
            'consumed_date') \
            .resample('M').agg(
            net_revenue=('netValue', 'sum'),
            gross_revenue=('grossValue', 'sum'),
            nb_bookings = ('grossValue', 'count')
            )
    print(1)



