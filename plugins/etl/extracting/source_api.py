# ERA5
import cdsapi

# time
year = '2024'
month = '05'
day = '16'

target_era5_pressure = f'/Users/thanhho/Documents/weather-data-warehouse/sources/era5-pressure/{year}{month}{day}.grib'


def fetch_era5_single_file_by_date(year, month, day):
    dataset = 'reanalysis-era5-single-levels'
    target_era5_single = f'/Users/luanluan/Documents/Data/dw_airflow_2/data/era5-single/{year}{month}{day}.grib'
    era5 = cdsapi.Client()
    era5.retrieve(
        dataset,
        {
            'product_type': 'reanalysis',
            'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
                '2m_temperature', 'mean_sea_level_pressure', 'mean_wave_direction',
                'mean_wave_period', 'sea_surface_temperature', 'significant_height_of_combined_wind_waves_and_swell',
                'surface_pressure', 'total_precipitation',
            ],
            'pressure_level': [
                '1', '2', '3',
                '5', '7', '10',
                '20', '30', '50',
                '70', '100', '125',
                '150', '175', '200',
                '225', '250', '300',
                '350', '400', '450',
                '500', '550', '600',
                '650', '700', '750',
                '775', '800', '825',
                '850', '875', '900',
                '925', '950', '975',
                '1000',
            ],
            'year': year,
            'month': month,
            'day': day,
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': [
                23.39, 102.14, 7.89,
                114.68,
            ],
            'format': 'grib',
        },
        target_era5_single
    )