class SqlQueries:
    pestcide_usage_table_insert = ("""
        INSERT INTO pestcide_use (compound_id, year, county_id, low_est, high_est)
        SELECT
                compounds.compound_id,
                pests.year,
                counties.county_id,
                pests.low_est,
                pests.high_est
            FROM staging_pestcide pests
            JOIN compounds
                ON pests.compound_name = compounds.compound_name
            JOIN counties
                ON pests.state_code = counties.state_code
                AND pests.county_code = counties.county_code;
    """)

    compound_table_insert = ("""
        INSERT INTO compounds (compound_name)
        SELECT distinct compound_name
        FROM staging_pestcide;
    """)

    county_table_insert = ("""
    INSERT INTO counties (state_code, county_code, county_name)
        SELECT distinct state_code, county_code, county_name
        FROM staging_dictionary;
    """)

    state_table_insert = ("""
        INSERT INTO states (state_code, state_name)
        SELECT distinct state_code, state_name
        FROM staging_dictionary;
    """)