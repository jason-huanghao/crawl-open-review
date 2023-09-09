import pandas as pd
import requests
import json
import threading
from time import sleep

# https://github.com/Hipo/university-domains-list
all_univs = json.load(open("result/world_universities_and_domains.json", "r", encoding='utf-8'))


def find_university_by_domain(domain):
    endpoint = r"http://universities.hipolabs.com/search"
    # find in local file
    for univ in all_univs:
        # print(univ['domains'])
        if domain in univ['domains']:
            return univ["name"]

    # find from endpoint
    session = requests.Session()
    university_data = session.get(endpoint, params={"domain": domain}, timeout=10).json()
    if not university_data:
        return None
    return university_data[0]['name']


def find_domain_by_university(u):
    endpoint = r"http://universities.hipolabs.com/search"
    # find from local file
    for univ in all_univs:
        if univ['name'].lower() == u.lower():
            return univ['domains']
    # find from endpoint
    session = requests.Session()
    university_data = session.get(endpoint, params={"name": u}, timeout=10).json()
    if not university_data:
        return None
    return university_data[0]['domains']


'''
#############################################################################
'''
author = pd.read_csv("archive_old/author.csv", sep=',',
                     converters={"emails": str, "institution_name": str, "institution_domain": str})
print("no institution author number:", len(author.loc[~author['institution_name'].isnull()]))


def convert_emails(e):
    if not e:
        return None
    if '[' in e:
        email_list = eval(e)
        return email_list
    else:
        return [e]


author_tmp = author[['author_id', 'emails', 'institution_name', 'institution_domain']].copy()
author_tmp['emails'] = author_tmp['emails'].apply(lambda x: convert_emails(x))
author_tmp['emails'] = author_tmp.apply(
    lambda x: x['emails'] if x['emails'] else [x['author_id']] if '@' in x['author_id'] else None, axis=1)

domain2univ = {}
univ2domain = {}


def add_univ(domain):
    if domain and domain not in domain2univ.keys():
        univ = find_university_by_domain(domain)
        if univ:
            domain2univ[domain] = univ
            univ2domain[univ] = domain
        else:
            domain2univ[domain] = None


def add_domain(univ):
    if univ and univ not in univ2domain.keys():
        domains = find_domain_by_university(univ)
        if not domains:
            univ2domain[univ] = None
            return
        for dom in domains:
            univ2domain[univ] = dom
            domain2univ[dom] = univ


# define a function that takes a row of author_tmp and finds the university name or domain
def find_univ_or_domain(row):
    print(row.name)
    domain = row['institution_domain']
    univ = row['institution_name']
    if row['emails'] and type(row['emails']) == list:
        email_domains = [e.split("@")[1] for e in row['emails'] if "@" in e]
        for email_domain in email_domains:
            add_univ(email_domain)

    add_univ(domain)
    add_domain(univ)


# create a list of threads by iterating over the rows of author_tmp
threads = []
for i, row in author_tmp.iterrows():
    thread = threading.Thread(target=find_univ_or_domain, args=(row,))
    threads.append(thread)

# start all the threads
for thread in threads:
    thread.start()
    sleep(0.01)

# wait for all the threads to finish
for thread in threads:
    thread.join()

# save the results to a csv file
univ_domain = set([(v, k) for k, v in domain2univ.items()]) | set([(k, v) for k, v in univ2domain.items()])

univ_df = pd.DataFrame([{"univ": u, "domain": d} for u, d in univ_domain]).to_csv(
    "result/university.csv", index=False)
