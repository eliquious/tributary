#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    import unittest2 as unittest
except ImportError:
    import unittest

from . import sample


class TestAwesomeBamCode(unittest.TestCase):
    """Tests the awesome wam-bam things in code."""

    def test_add(self):
        # Create an object
        samp_obj = sample.AwesomeBamCode()

        # Check that our addition worked
        self.assertEquals(samp_obj.add(2), 2)

        # Check that our addition worked
        self.assertEquals(samp_obj.add(2), 4)

    def test_add_type(self):
        # Create an object
        samp_obj = sample.AwesomeBamCode()

        # Check that we wanted a number
        self.assertRaises(ValueError, samp_obj.add, 'b')

    def test_sub(self):
        # Create an object
        samp_obj = sample.AwesomeBamCode()

        # Check that our subitution worked
        self.assertEquals(samp_obj.sub(2), -2)

        # Check that our substition worked
        self.assertEquals(samp_obj.sub(2), -4)

    def test_sub_type(self):
        # Create an object
        samp_obj = sample.AwesomeBamCode()

        # Check that we wanted a number
        self.assertRaises(ValueError, samp_obj.sub, 'b')
