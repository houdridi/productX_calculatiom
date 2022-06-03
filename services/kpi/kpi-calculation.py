import pandas as pd
import logging
from utils import (process_reservations_data,
                    process_availability_space
                  )
import CONSTANTS

class KpiCalculator:

    def __init__(
            self,
            hotel_id: str,
            reservation_path: str,
            space_availability_path: str
    ):
        self.hotel_id=hotel_id
        self.reservation_path=reservation_path
        self.space_availability_path=space_availability_path

        # create the flat dataFrame
        self._create_flat_dataFrame()

        # create the revenue and the rooms availability per month
        self._calculate_revenue_nb_bookings_rms_available()

        # calculate the occupancy
        self._calculate_occupancy()

        # calculate the ADR
        self._calculate_adr()

        # calculate the revPAR
        self._calculate_RevPAR()

    def _create_flat_dataFrame(self):
        # Process reservation raw data
        self.df_reservations = process_reservations_data(self.reservation_path)

        # Process space_availability raw data
        self.df_space_availability = process_availability_space(self.space_availability_path)

        # merge all needed information
        self.flat_dataFrame = self.df_reservations.merge(self.df_space_availability, how='left', on='consumed_date')

    def _filter_reservation(self, method='default'):
        pass

    # caluculate revenue per month
    def _calculate_revenue_nb_bookings_rms_available(self):
        self._df_revenue_daily = self.flat_dataFrame[['consumed_date', 'netValue', 'grossValue', 'rooms_available'
                                                       ]].set_index(
            'consumed_date') \
            .resample('D').agg(
            net_revenue=('netValue', 'sum'),
            gross_revenue=('grossValue', 'sum'),
            nb_bookings = ('grossValue', 'count'),
            rooms_available = ('rooms_available', 'mean')
        )

        df_total_room_available_per_month = self.df_space_availability.set_index(
            'consumed_date').resample('M').\
            agg(tot_rooms_available = (
            'rooms_available', 'sum'))\

        self._df_revenue_monthly = self.flat_dataFrame[['consumed_date', 'netValue', 'grossValue', 'rooms_available'
                                                       ]].set_index(
            'consumed_date') \
            .resample('M').agg(
            net_revenue=('netValue', 'sum'),
            gross_revenue=('grossValue', 'sum'),
            nb_bookings = ('grossValue', 'count'),
            rooms_available = ('rooms_available', 'sum')
        )

        self._df_revenue_monthly = self._df_revenue_monthly.merge(df_total_room_available_per_month,\
                                                                how='left', \
                                                                left_index=True,\
                                                                right_index=True)

    def _calculate_adr(self):
        #caculate monthly ADR
        self._df_revenue_monthly['adr'] = self._df_revenue_monthly['net_revenue']\
            .div(self._df_revenue_monthly['nb_bookings'])

        # caculate daily ADR
        self._df_revenue_daily['adr'] = self._df_revenue_daily['net_revenue'] \
            .div(self._df_revenue_daily['nb_bookings'])

    def _calculate_occupancy(self):
        #caculate monthly Occupancy
        self._df_revenue_monthly['occ'] = self._df_revenue_monthly['nb_bookings']\
            .div(self._df_revenue_monthly['rooms_available'])

        # caculate daily Occupancy
        self._df_revenue_daily['occ'] = self._df_revenue_daily['nb_bookings'] \
            .div(self._df_revenue_daily['rooms_available'])

    def _calculate_RevPAR(self):
        # caculate monthly RevPAR
        self._df_revenue_monthly['revPAR'] = self._df_revenue_monthly['net_revenue'] \
            .div(self._df_revenue_monthly['rooms_available'])

        # caculate daily RevPAR
        self._df_revenue_daily['revPAR'] = self._df_revenue_daily['net_revenue'] \
            .div(self._df_revenue_daily['rooms_available'])

    def get_monthly_KPI_dataframe(self):
        return self._df_revenue_monthly

    def get_daily_KPI_dataframe(self):
        return self._df_revenue_daily

file_path_reservation = '/Users/houssemeddinedridi/PycharmProjects/productX-calculation/data/reservations-s3'
file_path_space_availability = '/Users/houssemeddinedridi/PycharmProjects/productX-calculation/data/space-availability-s3'

kpi_cal = KpiCalculator(None, file_path_reservation, file_path_space_availability)
print(1)
