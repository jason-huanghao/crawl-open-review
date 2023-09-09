import threading
import openreview
import pandas as pd
import joblib


def extract_authors_infos():
    client = openreview.Client(baseurl='https://api.openreview.net',
                               username="jason.hao.academic@gmail.com",
                               password="jason970504")

    papers = pd.read_csv("result/papers.csv", usecols=['authorids', 'authors'],
                         converters={'authorids': eval, 'authors': eval})

    def get_from_profile(content, k):
        if k not in content.keys():
            return None
        else:
            return content[k]

    done_authorids = set()
    author_list = []

    # Define a function that extracts the information for one author
    def extract_one_author(author_id, i, authors):
        tmp = {}
        if author_id in done_authorids:
            return
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
            tmp['emails'] = None if '@' not in author_id else [author_id]
            tmp['dblp'] = None
            tmp['google_scholar'] = None
            tmp['home_page'] = None
            tmp['institution_domain'] = None
            tmp['institution_name'] = None
            author_list.append(tmp)
            return
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

        name = None if name is None else ' '.join(
            ['' if not name['middle'] else name['middle'], name['last'], name['first']]).strip()
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

    # Create an empty list to store the threads
    threads = []

    # Loop through the papers and create a thread for each author id
    for author_ids, authors in zip(papers['authorids'], papers['authors']):
        for i, author_id in enumerate(author_ids):
            # Create a thread object with the function and arguments
            thread = threading.Thread(target=extract_one_author, args=(author_id, i, authors))

            # Append the thread to the list
            threads.append(thread)

            # Start the thread
            thread.start()

    # Join the threads to ensure they finish before the main thread
    for thread in threads:
        thread.join()

    joblib.dump(author_list, 'RreviewData/data_fetch/archive_old/author.job')
    pd.DataFrame(author_list).to_csv('RreviewData/data_fetch/archive_old/author.csv', index=False)


# Call the main function
extract_authors_infos()
