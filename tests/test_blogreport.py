# -*- coding: utf-8 -*-

"""
  unit tests for blogreport.py
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for blogreport.py.

"""
import unittest
import nose
import datetime
import blogreport


class UtilTestCase(unittest.TestCase):
    def test_parse_string_to_date_yesterday(self):
        actual = blogreport.parse_string_to_date('yesterday')

        expected = (datetime.datetime.utcnow().date()
                    - datetime.timedelta(days=1))
        self.assertEqual(actual, expected)

    def test_parse_string_to_date_2014_11_08(self):
        actual = blogreport.parse_string_to_date('2014-11-08')

        expected = datetime.date(2014, 11, 8)
        self.assertEqual(actual, expected)

    def test_parse_string_to_date_bogus_date(self):
        nose.tools.assert_raises(ValueError,
                                 blogreport.parse_string_to_date, 'foo')
