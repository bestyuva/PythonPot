import psycopg2
import psycopg2.extras
import allure
import sys
import os
import unittest
import pytest
from Custom_DataTest.con_config import config
from datetime import datetime
import logging
import pytest_check as check

@allure.severity(allure.severity_level.MINOR)
class Test_Metro_Area_Datatest:
    params = config()
    print('Connecting to the Database...')
    connection = psycopg2.connect(**params)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
    cursor.close()

    @allure.severity(allure.severity_level.NORMAL)
    @pytest.mark.run(order=1)
    def test_1_Nulldata_Check(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgreSQL_select_Query1 = "select * from public.sixwallz_app_neighbourhood where affordability_data='[]'"
        cursor.execute(postgreSQL_select_Query1)
        print("Selecting rows from sixwallz_app_neighbourhood table using cursor.fetchall")
        metro_records1 = cursor.fetchall()
        print('_____________ResultCount____________ = %d' % len(metro_records1))
        print("Print each row and it's columns values")
        for row in metro_records1:
            print(f"Id = ", row[0], )
            print(f"neighbour_name = ", row[1])
            print(f"affordability_data  = ", row[2], "\n")
        check.equal(len(metro_records1) == 0, "Results Received for NULL'[]' data, Hence case Failed")
        cursor.close()

    @allure.severity(allure.severity_level.NORMAL)
    @pytest.mark.run(order=2)
    def test_2_Available_Neighbourhood_Check(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgreSQL_select_Query2 = "select * from public.sixwallz_app_neighbourhood where affordability_data !='[]'"
        cursor.execute(postgreSQL_select_Query2)
        print("Selecting rows from sixwallz_app_neighbourhood table using cursor.fetchall")
        metro_records2 = cursor.fetchall()
        print('_____________ResultCount____________ = %d' % len(metro_records2))
        print("Print each row and it's columns values")
        for row in metro_records2:
            print(f"Id = ", row[0], )
            print(f"neighbour_name = ", row[1])
            print(f"affordability_data  = ", row[2], "\n")
        cursor.close()

    @pytest.mark.run(order=3)
    def test_3_Completed(self):
        if self.connection is not None:
            self.connection.close()
            print('database Connection Closed')


if __name__ == "__main__":
    test_connect(self)
    
