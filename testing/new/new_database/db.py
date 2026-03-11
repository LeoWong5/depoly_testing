import sqlite3
import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_PATH    = os.path.join(_SCRIPT_DIR, "nptg_naptan.db")


def init_db():
    # Remove stale DB from a previous run so we start fresh
    if os.path.exists(_DB_PATH):
        os.remove(_DB_PATH)

    conn = sqlite3.connect(_DB_PATH)
    cursor = conn.cursor()

    # Enable foreign key support in SQLite
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Region (The Root)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Region (
            Reg_code CHAR(2) PRIMARY KEY,
            Reg_name CHAR(20) NOT NULL,
            Reg_country CHAR(20) NOT NULL
        )
    ''')

    # 2. Authority
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Authority (
            Aut_admin_area_code CHAR(10) PRIMARY KEY,
            Reg_code CHAR(2) NOT NULL,
            Aut_atco_area_code CHAR(10) UNIQUE NOT NULL,
            Aut_name CHAR(100) NOT NULL,
            Aut_short_name CHAR(100) NOT NULL,
            Aut_national BOOLEAN NOT NULL,
            FOREIGN KEY (Reg_code) REFERENCES Region (Reg_code)
        )
    ''')

    # 3. District
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS District (
            DIS_nptg_code CHAR(10) PRIMARY KEY,
            Aut_admin_area_code CHAR(10) NOT NULL,
            DIS_name CHAR(100) NOT NULL,
            FOREIGN KEY (Aut_admin_area_code) REFERENCES Authority (Aut_admin_area_code)
        )
    ''')

    # 4. Locality (Includes self-reference for parent_locality_ref)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Locality (
            LOC_nptg_code CHAR(15) PRIMARY KEY,
            LOC_parent_locality_ref CHAR(15),
            Aut_admin_area_code CHAR(10) NOT NULL,
            DIS_nptg_code CHAR(10) NOT NULL,
            LOC_name CHAR(100) NOT NULL,
            LOC_qualifier_name CHAR(100),
            LOC_type CHAR(10) NOT NULL,
            LOC_longitude FLOAT NOT NULL,
            LOC_latitude FLOAT NOT NULL,
            FOREIGN KEY (LOC_parent_locality_ref) REFERENCES Locality (LOC_nptg_code),
            FOREIGN KEY (Aut_admin_area_code) REFERENCES Authority (Aut_admin_area_code),
            FOREIGN KEY (DIS_nptg_code) REFERENCES District (DIS_nptg_code)
        )
    ''')

    # 5. Stop_Area
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Stop_Area (
            SA_code CHAR(20) PRIMARY KEY,
            Aut_admin_area_code CHAR(10) NOT NULL,
            SA_name CHAR(50) NOT NULL,
            SA_type CHAR(10) NOT NULL,
            SA_longitude FLOAT,
            SA_latitude FLOAT,
            FOREIGN KEY (Aut_admin_area_code) REFERENCES Authority (Aut_admin_area_code)
        )
    ''')

    # 6. Stop_Point (The "Big Table")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Stop_Point (
            SP_atco_code CHAR(20) PRIMARY KEY,
            Aut_atco_area_code CHAR(10) NOT NULL,
            Aut_admin_area_code CHAR(10) NOT NULL,
            SA_code CHAR(20),
            LOC_nptg_code CHAR(15) NOT NULL,
            SP_naptan_code CHAR(20),
            SP_plus_bus_zone_ref CHAR(20),
            SP_start_date DATE,
            SP_name CHAR(100) NOT NULL,
            SP_landmark CHAR(100),
            SP_street CHAR(100),
            SP_indicator CHAR(10),
            SP_missing_data BOOLEAN NOT NULL DEFAULT 0,
            SP_longitude FLOAT,
            SP_latitude FLOAT,
            SP_stop_type CHAR(10) NOT NULL,
            SP_bus_stop_type CHAR(10),
            SP_timing_status CHAR(10),
            SP_compass_point CHAR(5),
            SP_degrees INTEGER,
            FOREIGN KEY (Aut_atco_area_code) REFERENCES Authority (Aut_atco_area_code),
            FOREIGN KEY (Aut_admin_area_code) REFERENCES Authority (Aut_admin_area_code),
            FOREIGN KEY (SA_code) REFERENCES Stop_Area (SA_code),
            FOREIGN KEY (LOC_nptg_code) REFERENCES Locality (LOC_nptg_code)
        )
    ''')

    
    # 7. Operator
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Operator (
            OPE_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            OPE_id CHAR(10) NOT NULL,
            OPE_national_code CHAR(10) NOT NULL,
            OPE_code CHAR(10) NOT NULL,
            OPE_short_name CHAR(30) NOT NULL,
            OPE_licence_number CHAR(30) NOT NULL,
            OPE_licence_name CHAR(30),
            OPE_trading_name CHAR(30)
        )
    ''')

    # 8. Garage
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Garage (
            GAR_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            OPE_UID INTEGER NOT NULL,
            GAR_code CHAR(10) NOT NULL,
            GAR_name CHAR(20),
            GAR_longitude FLOAT,
            GAR_latitude FLOAT,
            FOREIGN KEY (OPE_UID) REFERENCES Operator (OPE_UID)
        )
    ''')

    # 9. Service
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Service (
            SER_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            OPE_UID INTEGER NOT NULL,
            SER_service_code CHAR(20) NOT NULL,
            SER_start_date DATE NOT NULL,
            SER_end_date DATE,
            OPE_code CHAR(10) NOT NULL,
            SER_origin CHAR(50) NOT NULL,
            SER_destination CHAR(50) NOT NULL,
            FOREIGN KEY (OPE_UID) REFERENCES Operator (OPE_UID)
        )
    ''')

    # 10. Line
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Line (
            LIN_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            SER_UID INTEGER NOT NULL,
            LIN_id CHAR(20) NOT NULL,
            LIN_name CHAR(20) NOT NULL,
            LIN_out_bound_orig CHAR(50),
            LIN_out_bound_dest CHAR(50),
            LIN_out_bound_desc CHAR(50),
            LIN_in_bound_orig CHAR(50),
            LIN_in_bound_dest CHAR(50),
            LIN_in_bound_desc CHAR(50),
            SER_service_code CHAR(20) NOT NULL,
            FOREIGN KEY (SER_UID) REFERENCES Service (SER_UID)
        )
    ''')

    # 11. Route
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Route (
            ROU_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            ROU_id CHAR(10) NOT NULL,
            ROU_private_code CHAR(20),
            ROU_description CHAR(50)
        )
    ''')

    # 12. Route_Section
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Route_Section (
            RSEC_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            ROU_UID INTEGER NOT NULL,
            RSEC_id CHAR(10) NOT NULL,
            FOREIGN KEY (ROU_UID) REFERENCES Route (ROU_UID)
        )
    ''')

    # 13. Bus_Stop_Point
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Bus_Stop_Point (
            Bus_SP_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            SP_atco_code CHAR(20) NOT NULL,
            Bus_SP_name CHAR(30) NOT NULL,
            FOREIGN KEY (SP_atco_code) REFERENCES Stop_Point (SP_atco_code)
        )
    ''')

    # 14. Route_Link
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Route_Link (
            RLIN_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            RSEC_UID INTEGER NOT NULL,
            from_SP_atco_code CHAR(20) NOT NULL,
            to_SP_atco_code CHAR(20) NOT NULL,
            from_Bus_SP_uid INTEGER,
            to_Bus_SP_uid INTEGER,
            RLIN_id CHAR(10) NOT NULL,
            RLIN_distance INTEGER,
            RLIN_global_seq INTEGER,
            FOREIGN KEY (RSEC_UID) REFERENCES Route_Section (RSEC_UID),
            FOREIGN KEY (from_SP_atco_code) REFERENCES Stop_Point (SP_atco_code),
            FOREIGN KEY (to_SP_atco_code) REFERENCES Stop_Point (SP_atco_code),
            FOREIGN KEY (from_Bus_SP_uid) REFERENCES Bus_Stop_Point (Bus_SP_UID),
            FOREIGN KEY (to_Bus_SP_uid) REFERENCES Bus_Stop_Point (Bus_SP_UID)
        )
    ''')

    # 15. Route_Location
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Route_Location (
            RLOC_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            RLIN_UID INTEGER NOT NULL,
            RLOC_id CHAR(10) NOT NULL,
            RLOC_longitude FLOAT NOT NULL,
            RLOC_latitude FLOAT NOT NULL,
            RLOC_global_seq INTEGER,
            FOREIGN KEY (RLIN_UID) REFERENCES Route_Link (RLIN_UID)
        )
    ''')

    # 16. Journey_Pattern
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Journey_Pattern (
            JP_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            OPE_UID INTEGER NOT NULL,
            SER_UID INTEGER NOT NULL,
            ROU_UID INTEGER NOT NULL,
            JP_id CHAR(10) NOT NULL,
            JP_dest_display CHAR(30) NOT NULL,
            JP_direction CHAR(20) NOT NULL,
            JP_description CHAR(50),
            FOREIGN KEY (OPE_UID) REFERENCES Operator (OPE_UID),
            FOREIGN KEY (SER_UID) REFERENCES Service (SER_UID),
            FOREIGN KEY (ROU_UID) REFERENCES Route (ROU_UID)
        )
    ''')

    # 17. Journey_Pattern_Section
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Journey_Pattern_Section (
            JPS_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            JP_UID INTEGER NOT NULL,
            JPS_id CHAR(10) NOT NULL,
            FOREIGN KEY (JP_UID) REFERENCES Journey_Pattern (JP_UID)
        )
    ''')

    # 18. Journey_Pattern_Link
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Journey_Pattern_Link (
            JPL_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            RLIN_UID INTEGER NOT NULL,
            JPS_UID INTEGER NOT NULL,
            JPL_from_point_atco_code CHAR(20) NOT NULL,
            JPL_to_point_atco_code CHAR(20) NOT NULL,
            from_sequence_num INTEGER NOT NULL,
            from_activity CHAR(20),
            from_dest_dispaly CHAR(30),
            from_timing_status CHAR(20) NOT NULL,
            from_fare_stage_num CHAR(10),
            to_sequence_num INTEGER NOT NULL,
            to_activity CHAR(20),
            to_dest_dispaly CHAR(30),
            to_timing_status CHAR(20) NOT NULL,
            to_fare_stage_num CHAR(10),
            JPL_run_time TIME NOT NULL,
            JPL_global_seq INTEGER,
            FOREIGN KEY (RLIN_UID) REFERENCES Route_Link (RLIN_UID),
            FOREIGN KEY (JPS_UID) REFERENCES Journey_Pattern_Section (JPS_UID),
            FOREIGN KEY (JPL_from_point_atco_code) REFERENCES Stop_Point (SP_atco_code),
            FOREIGN KEY (JPL_to_point_atco_code) REFERENCES Stop_Point (SP_atco_code)
        )
    ''')

    # 19. Serviced_Organisation
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Serviced_Organisation (
            SO_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            SO_code CHAR(20) NOT NULL,
            SO_name CHAR(30) NOT NULL
        )
    ''')

    # 20. Serviced_Organisation_Date_Range
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Serviced_Organisation_Date_Range (
            SODR_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            SO_UID INTEGER NOT NULL,
            SODR_start_date DATE NOT NULL,
            SODR_end_date DATE NOT NULL,
            SODR_description CHAR(30) NOT NULL,
            FOREIGN KEY (SO_UID) REFERENCES Serviced_Organisation (SO_UID)
        )
    ''')

    # 21. Days_Of_Week
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Days_Of_Week (
            DOW_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            VJ_UID INTEGER NOT NULL,
            DOW_monday BOOLEAN NOT NULL,
            DOW_tuesday BOOLEAN NOT NULL,
            DOW_wednesday BOOLEAN NOT NULL,
            DOW_thursday BOOLEAN NOT NULL,
            DOW_friday BOOLEAN NOT NULL,
            DOW_saturday BOOLEAN NOT NULL,
            DOW_sunday BOOLEAN NOT NULL,
            DOW_days INTEGER,
            FOREIGN KEY (VJ_UID) REFERENCES Vehicle_Journey (VJ_UID)
        )
    ''')

    # 22. Special_Days_Operation
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Special_Days_Operation (
            SDO_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            VJ_UID INTEGER NOT NULL,
            SDO_do_operate BOOLEAN NOT NULL,
            SDO_start_date DATE NOT NULL,
            SDO_end_date DATE NOT NULL,
            FOREIGN KEY (VJ_UID) REFERENCES Vehicle_Journey (VJ_UID)
        )
    ''')

    # 23. Bank_Holiday_Operation
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Bank_Holiday_Operation (
            BHO_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            VJ_UID INTEGER NOT NULL,
            BHO_days_of_operation TEXT,
            BHO_days_of_non_operation TEXT,
            FOREIGN KEY (VJ_UID) REFERENCES Vehicle_Journey (VJ_UID)
        )
    ''')

    # 24. Vehicle_Journey_Link
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Vehicle_Journey_Link (
            VJL_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            VJ_UID INTEGER NOT NULL,
            JPL_UID INTEGER NOT NULL,
            VJL_id CHAR(10) NOT NULL,
            VJL_run_time TIME NOT NULL,
            VJL_from_activity CHAR(20),
            VJL_to_activity CHAR(20),
            VJL_global_seq INTEGER,
            FOREIGN KEY (VJ_UID) REFERENCES Vehicle_Journey (VJ_UID),
            FOREIGN KEY (JPL_UID) REFERENCES Journey_Pattern_Link (JPL_UID)
        )
    ''')

    # 25. Vehicle_Journey
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Vehicle_Journey (
            VJ_UID INTEGER PRIMARY KEY AUTOINCREMENT,
            OPE_UID INTEGER NOT NULL,
            SO_UID INTEGER,
            GAR_UID INTEGER,
            SER_UID INTEGER NOT NULL,
            LIN_UID INTEGER NOT NULL,
            JP_UID INTEGER NOT NULL,
            VJ_private_code CHAR(50),
            VJ_sequence_number INTEGER NOT NULL,
            VJ_code CHAR(10) NOT NULL,
            departure_time TIME NOT NULL,
            FOREIGN KEY (OPE_UID) REFERENCES Operator (OPE_UID),
            FOREIGN KEY (SO_UID) REFERENCES Serviced_Organisation (SO_UID),
            FOREIGN KEY (GAR_UID) REFERENCES Garage (GAR_UID),
            FOREIGN KEY (SER_UID) REFERENCES Service (SER_UID),
            FOREIGN KEY (LIN_UID) REFERENCES Line (LIN_UID),
            FOREIGN KEY (JP_UID) REFERENCES Journey_Pattern (JP_UID)
        )
    ''')

    conn.commit()
    conn.close()
    print("✅ Full NPTG/NaPTAN and Bus Timetable Schema Created Successfully!")

# if __name__ == "__main__":
#     init_db()
