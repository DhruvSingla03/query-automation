class Product:
    CODE = 'FASTAG_ACQ'
    FOLDER = 'fastagacq'
    DISPLAY_NAME = 'FASTag Acquiring'
    VAULT_PATH = 'secret/data/fastag_acq/database'

class Tables:
    PLAZA = 'NETCACQ_PLAZA_DTLS'
    CONCESSIONAIRE = 'NETCACQ_PLAZA_CONCESSION_DTLS'
    LANE = 'NETCACQ_PLAZA_LANE_DTLS'
    FARE = 'NETCACQ_PLAZA_FARE_DTLS'
    VEHICLE_MAPPING = 'NETCACQ_VHCLCLASS_MAPPING_DTLS'
    USER_MAPPING = 'NETCACQ_USER_ROLE_MAPPING_DTLS'

class FieldRules:
    MUTABLE = {
        Tables.PLAZA: ['modified_ts'],
        Tables.CONCESSIONAIRE: ['modified_ts'],
        Tables.LANE: ['modified_ts'],
        Tables.FARE: ['modified_ts'],
        Tables.VEHICLE_MAPPING: ['modified_ts'],
        Tables.USER_MAPPING: ['modified_ts']
    }
