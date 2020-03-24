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
from hdx.data.user import User
from hdx.facades.simple import facade
from hdx.hdx_configuration import Configuration
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date

from activity import Activity

logger = logging.getLogger(__name__)


def get_user_name(user):
    user_name = user.get('display_name')
    if not user_name:
        user_name = user['fullname']
        if not user_name:
            user_name = user['name']
    return user_name


def main():
    configuration = Configuration.read()
    enddays = configuration['enddays']
    ignore_users = configuration['ignore_users']
    users_scrapers = configuration['users_scrapers']
    spreadsheet_url = configuration['spreadsheet_url']
    sheetname = configuration['sheetname']
    logger.info('> GSheet Credentials: %s' % gsheet_auth)
    users = dict()
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
        iso3s = list()
        for country in data['countries']:
            iso3, _ = Country.get_iso3_country_code_fuzzy(country)
            iso3s.append('groups:%s' % iso3.lower())
        search_string = 'metadata_created:[2000-01-01T00:00:00.000Z TO %sZ] AND (%s)' % (enddate.isoformat(), ' OR '.join(iso3s))
        datasets = Dataset.search_in_hdx(fq=search_string)
        row = {'ID': data['id'], 'Crisis name': crisis}
        count = 0
        largest_activities = 0
        for dataset in datasets:
            metadata_created_str = dataset['metadata_created']
            orgname = dataset['organization']['name']
            metadata_created = parse_date(metadata_created_str)
            new_or_updated = 'new'
            updated_when = ''
            updated_by = ''
            if metadata_created < startdate:
                activities = Activity.get_all_activities(id=dataset['id'], limit=10000)
                activities_len = len(activities)
                if activities_len > largest_activities:
                    largest_activities = activities_len
                found = False
                for activity in activities:
                    timestamp = activity['timestamp']
                    activity_date = parse_date(timestamp)
                    if startdate < activity_date < enddate:
                        new_or_updated = 'updated'
                        updated_when = timestamp
                        user_id = activity['user_id']
                        check_ignore = True
                        for user_scrapers in users_scrapers:
                            if user_id == user_scrapers['id']:
                                if orgname in user_scrapers['scrapers']:
                                    check_ignore = False
                                    break
                        if check_ignore:
                            if user_id in ignore_users:
                                continue
                        username = users.get(user_id)
                        if username is None:
                            user = User.read_from_hdx(user_id)
                            username = get_user_name(user)
                            users[user_id] = username
                        updated_by = username
                        found = True
                        break
                if not found:
                    continue
            row['dataset title'] = dataset['title']
            row['dataset id'] = dataset['id']
            row['dataset url'] = dataset.get_hdx_url()
            row['org name'] = orgname
            row['org id'] = dataset['organization']['id']
            row['created'] = metadata_created_str
            row['new or updated'] = new_or_updated
            row['updated when'] = updated_when
            row['updated by'] = updated_by
            rows.append([row.get(key, '') for key in keys])
            count += 1
        logger.info('%s: %d\t%s' % (crisis, count, search_string))
    sheet.clear()
    sheet.update_values('A1', rows)
    logger.info('Longest activities: %d' % largest_activities)


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
