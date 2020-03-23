#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script.

"""
import argparse
import json
import logging
from datetime import timedelta
from os import getenv
from os.path import join

import pygsheets
from google.oauth2 import service_account
from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


def main():
    configuration = Configuration.read()
    enddays = configuration['enddays']
    spreadsheet_url = configuration['spreadsheet_url']
    sheetname = configuration['sheetname']
    logger.info('> GSheet Credentials: %s' % gsheet_auth)
    info = json.loads(gsheet_auth)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    gc = pygsheets.authorize(custom_credentials=credentials)
    spreadsheet = gc.open_by_url(spreadsheet_url)
    sheet = spreadsheet.worksheet_by_title(sheetname)
    keys = sheet.get_row(1)
    rows = [keys]
    crisisdata = configuration['crisisdata']
    for crisis in crisisdata:
        data = crisisdata[crisis]
        startdate = parse_date(data['startdate'])
        enddate = startdate + timedelta(days=enddays)
        startdatestr = '%sZ' % startdate.isoformat()
        enddatestr = '%sZ' % enddate.isoformat()
        iso3s = list()
        for country in data['countries']:
            iso3, _ = Country.get_iso3_country_code_fuzzy(country)
            iso3s.append('groups:%s' % iso3.lower())
        search_string = '%s' % (startdatestr, enddatestr, ' OR '.join(iso3s))
        datasets = Dataset.search_in_hdx(fq=search_string)
        row = {'ID': data['id'], 'Crisis name': crisis, 'new or updated': 'new'}
        for dataset in datasets:
            row['dataset title'] = dataset['title']
            row['dataset id'] = dataset['id']
            row['dataset url'] = dataset.get_hdx_url()
            row['org name'] = dataset['organization']['name']
            row['org id'] = dataset['organization']['id']
            rows.append([row.get(key, '') for key in keys])
        logger.info('%s: %d\t%s' % (crisis, len(datasets), search_string))
    sheet.clear()
    sheet.update_values('A1', rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness Emailer')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-gs', '--gsheet_auth', default=None, help='Credentials for accessing Google Sheets')
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'crisis-casestudy'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv('GSHEET_AUTH')
    facade(main, hdx_key=hdx_key, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site, gsheet_auth=gsheet_auth, project_config_yaml=join('config', 'project_configuration.yml'))
