import joblib
import pandas as pd
from itertools import zip_longest
import json
import openreview


def rating_replace():
    rating_mappings = {
        # out of 5
        '1: Trivial or wrong': 0. / 4,
        '1: Strong Reject': 0. / 4,
        '2: Weak Reject': 1. / 4,
        '2: Reject': 1. / 4,
        '2: Marginally below acceptance threshold': 2. / 5,
        '3: Accept': 3. / 5,
        '3: Borderline': 1. / 2,
        '3: Marginally above acceptance threshold': 3. / 5,
        '4: Top 50% of accepted papers, clear accept': 4. / 5,
        '4: Strong accept': 4. / 5,
        '5: Top 15% of accepted papers, strong accept': 5. / 5,
        '5: Strong Accept': 5. / 5,
        # out of 10
        '1: Strong rejection': 0. / 9,
        '1: Strong reject': 0. / 9,
        '2: Strong rejection': 1. / 9,
        '3: Clear rejection': 2. / 9,
        '4: Weak Accept': 5. / 9,
        '4: Ok but not good enough - rejection': 3. / 9,
        '5: Marginally below acceptance threshold': 4. / 9,
        '6: Marginally above acceptance threshold': 5. / 9,
        '7: Good paper, accept': 6. / 9,
        '8: Top 50% of accepted papers, clear accept': 7. / 9,
        '9: Top 15% of accepted papers, strong accept': 8. / 9,
        '10: Top 5% of accepted papers, seminal paper': 9. / 9,
    }
    review_df = pd.read_csv("result/review.csv")
    if 'rating' in review_df.columns:
        return
    review_df['rating'] = review_df['rating_text'].apply(lambda x: rating_mappings[x] if x in rating_mappings.keys() else None)
    review_df.to_csv('result/review.csv', index=False)


def extract_authors_infos():
    client = openreview.Client(baseurl='https://api.openreview.net',
                               username="jason.hao.academic@gmail.com",
                               password="jason970504")

    def list_converter(s):
        return eval(s)
    papers = pd.read_csv("result/papers.csv", usecols=['authorids', 'authors'], converters={'authorids': list_converter, 'authors': list_converter})

    def get_from_profile(content, k):
        if k not in content.keys():
            return None
        else:
            return content[k]

    done_authorids = set()
    author_list = []
    for author_ids, authors in zip(papers['authorids'], papers['authors']):
        for i, author_id in enumerate(author_ids):
            tmp = {}
            if author_id in done_authorids:
                continue
            else:
                done_authorids.add(author_id)
            print(len(done_authorids))
            try:
                profile_cont = client.get_profile(author_id).content
            except:
                tmp['author_id'] = author_id
                tmp['gender'] = None
                tmp['name'] = None
                tmp['paper_name'] = authors[i] if len(authors) > i else None
                tmp['expertise'] = None
                tmp['emails'] = None if '@' not in author_id else author_id
                tmp['dblp'] = None
                tmp['google_scholar'] = None
                tmp['home_page'] = None
                tmp['institution_domain'] = None
                tmp['institution_name'] = None
                author_list.append(tmp)
                continue
            dblp = get_from_profile(profile_cont, 'dblp')
            google_scholar = get_from_profile(profile_cont, 'gscholar')
            gender = get_from_profile(profile_cont, 'gender')
            gender = None if gender is None else gender.lower()

            history = get_from_profile(profile_cont, 'history')
            inst_dict = history[0]['institution'] if history else None
            if inst_dict is not None:
                institution_domain = get_from_profile(inst_dict, 'domain')
                institution_name = get_from_profile(inst_dict, 'name')
            else:
                institution_domain, institution_name = None, None
            home_page = get_from_profile(profile_cont, 'homepage')
            name = get_from_profile(profile_cont, 'names')[0]
            expertise = get_from_profile(profile_cont, 'expertise')
            expertise = list(set([e for exp in expertise for e in exp['keywords']])) if expertise else None

            name = None if name is None else ' '.join(['' if not name['middle'] else name['middle'], name['last'], name['first']]).strip()
            emails = get_from_profile(profile_cont, 'emails')
            emails = None if emails is None else set(emails)
            if '@' in author_id:
                emails |= set([author_id])

            tmp['author_id'] = author_id
            tmp['gender'] = gender
            tmp['name'] = name
            tmp['paper_name'] = authors[i] if len(authors) > i else None
            tmp['expertise'] = expertise
            tmp['emails'] = list(emails)
            tmp['dblp'] = dblp
            tmp['google_scholar'] = google_scholar
            tmp['home_page'] = home_page
            tmp['institution_domain'] = institution_domain
            tmp['institution_name'] = institution_name

            author_list.append(tmp)
    joblib.dump(author_list, 'RreviewData/data_fetch/archive_old/author.job')
    pd.DataFrame(author_list).to_csv('RreviewData/data_fetch/archive_old/author.csv', index=False)


# Step 2.1:
rating_replace()
# STep 2.2:
# extract_authors_infos() # this method is too slow






'''
def fetch_info_scopus():
    import json
    import pprint
    # import signal
    import pybliometrics

    from pybliometrics.scopus.exception import Scopus429Error, ScopusQueryError, Scopus400Error
    from requests.exceptions import ConnectionError
    # from pybliometrics.scopus.utils import config
    # print(config['Authentication']['APIKey'])

    print = pprint.pprint
    table = str.maketrans(dict.fromkeys('*'))

    def handler(signum, frame):
        raise TimeoutError("scopus request timeout")

    with open("result/authors-temp.json", "r", encoding='utf-8') as f:
        data = json.load(f)

    for a in data:
        try:
            if "scopus" in a or not a["name"]:
                continue

            name = a["name"].translate(table).split(" ")
            last = name[-1]
            first = " ".join(name[:-1])

            if not (first and last):
                a["scopus"] = None
                continue
            try:
                query = f"AUTHLAST({last}) and AUTHFIRST({first}) and SUBJAREA(COMP)"
                print(query)
                reply = pybliometrics.scopus.AuthorSearch(query=query, count=1, download=True, max_entries=5000, subscriber=True) #, max_entries=1e6, subscriber=True)
            except (TimeoutError, ConnectionError):
                continue
            except (Scopus400Error, ScopusQueryError):
                print(f"invalid query: {query}")
                continue

            if not reply.get_results_size() or not reply.authors[0].eid:
                a["scopus"] = None
                continue
            id = reply.authors[0].eid

            result = pybliometrics.scopus.AuthorRetrieval(id)
            a["scopus"] = result.__dict__
            print(result.__dict__)
            input()

        except Scopus429Error:
            # our API key is exhausted; write what we have to disk
            print("[!] please rotate API key")
            break

    with open("result/authors-temp.json", "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

'''