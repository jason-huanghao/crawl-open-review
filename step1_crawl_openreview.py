import openreview
import pandas as pd

client = openreview.Client(baseurl='https://api.openreview.net',
        username="jason.hao.academic@gmail.com",
        password="jason970504")


def get_all_venues():
    # API V2
    all_venues = client.get_group(id='venues').members
    # all_single = client.get_group(id='venues').nonreaders
    for e in all_venues:
        print(e)
        # print(e, '\t', "single" if e in all_single else "double")
    # exit()

    keep_only = ['ICML', 'ICLR', 'ESWC', 'ISWC', 'EMNLP', 'KDD.org', 'ACM.org', 'NeurIPS', 'aclweb', 'AAAI']

    def check_keep(e: str):
        e = e.lower()
        for k in keep_only:
            if k.lower() in e:
                if "conference" in e or "workshop" in e:
                    if "2019" in e or "2020" in e or "2021" in e or "2022" in e:
                        return True
        return False
    return [e for e in all_venues if check_keep(e)]


def get_open_review_infos(venue):
    '''
    :param venue:
    :return:

    submission:
        content.abstract
        content.authorids
        content.authors
        content.title

        details.

        id / forum

        venue: venue
        blind: single, double
    '''
    blind = "single"
    submissions = client.get_all_notes(
        invitation=f"{venue}/-/Submission",
        details='directReplies'
    )
    print(venue, blind, "size:", len(submissions))
    if len(submissions) == 0:
        submissions = client.get_all_notes(
            invitation=f"{venue}/-/Blind_Submission",
            details='directReplies'
        )
        blind = "double"
        print(venue, blind, "size:", len(submissions))

    if len(submissions) == 0:   # no data available
        return None, None, None, None

    reviews = []
    for submission in submissions:
        reviews = reviews + [reply for reply in submission.details["directReplies"] if
                             reply["invitation"].endswith("Official_Review")]

    papers = {}
    reviews = {}
    decisions = {}

    decision_num = 0
    accept_num = 0
    # API V1
    for submission in submissions:
        abstract = None if 'abstract' not in submission.content.keys() else submission.content['abstract']
        authorids = submission.content['authorids']
        authors = submission.content['authors']
        title = submission.content['title']
        paper_id = submission.forum

        papers[paper_id] = {"title": title, "abstract": abstract, "authorids": authorids, "authors": authors, "conference": venue}

        review_list = [reply for reply in submission.details["directReplies"] if
         reply["invitation"].endswith("Official_Review")]
        if len(review_list) == 0:
            continue

        for review in review_list:
            review_id = review['id']
            paper_id = review['forum']
            rating_text = None if 'rating' not in review['content'].keys() else review['content']['rating']
            review_title = None if 'title' not in review['content']. keys() else review['content']['title']
            content = None if 'review' not in review['content'].keys() else review['content']['review']
            reviews[review_id] = {"paper_id": paper_id, "rating_text": rating_text, "review_title": review_title, "review": content}

        for dec in [reply for reply in submission.details["directReplies"] if
                                 reply["invitation"].endswith("Decision")]:
            decision_id = dec['id']
            paper_id = dec['forum']
            decision_text = dec['content']['decision']
            # comment = dec['content']['comment']
            decisions[decision_id] = {"paper_id": paper_id, "decision_text": decision_text}
            decision_num += 1
            if 'accept' in decision_text.lower():
                accept_num += 1

    print("decision number:", decision_num, "submission number:", len(submissions))
    confs = {venue: {"blind": blind,
                     "accept_rate": accept_num * 1.0 / len(submissions),
                     "submit_count": len(submissions),
                     "accept_count": accept_num,
                     "reject_count": len(submissions)-accept_num}}

    return papers, reviews, decisions, confs


def get_all_data():

    pas = {}
    rews = {}
    decs = {}
    confs = {}

    venues = get_all_venues()

    print("number of venues", len(venues))

    for venue in venues:
        pa, rew, dec, conf = get_open_review_infos(venue)
        if pa is None:
            continue
        pas = {**pas, **pa}
        rews = {**rews, **rew}
        decs = {**decs, **dec}
        confs = {**confs, **conf}

    def dict2df(dct_: dict, label, fn):
        df = pd.DataFrame([{**{label+"_id": k}, **v} for k, v in dct_.items()])
        df.to_csv(fn, index=False)

    print("number of papers", len(pas.keys()))

    dict2df(pas, "paper", "result/papers.csv")
    dict2df(rews, "review", "result/review.csv")
    dict2df(decs, "decision", "result/decision.csv")
    dict2df(confs, 'conf', "result/conference.csv")


get_all_data()
# get_open_review_infos("NIPS.cc/2018/Workshop/MLITS")









