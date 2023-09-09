import pandas as pd
from rdflib import namespace
import rdflib
import re


ex = namespace.Namespace('http://www.example.org/')
brief2url = {"ex": ex,
        "rdf": namespace.RDF,
        "rdfs": namespace.RDFS,
        "owl": namespace.OWL,
        "xsd": namespace.XSD}


# g = rdflib.Graph()
# for prefix, uri in brief2url.items():
#     g.bind(prefix, uri)

url2breif = {v: k for k, v in brief2url.items()}
blank_indx = -1


def new_blank_node():
    global blank_indx
    blank_indx += 1
    return rdflib.BNode("blank"+str(blank_indx))


def to_breif(txt):
    for k, v in brief2url.items():
        v = str(v)
        if v in txt:
            if '/' in txt.replace(v, ''):
                return txt
            else:
                return txt.replace(v, k+":").replace("<", "").replace(">", "")
    return txt


def rdf2tuple(rdf_tup):
    # print(rdf_tup)
    return tuple(to_breif(term.n3()) for term in rdf_tup)


def special_char_replace(s):
    # pattern = r"[,’#\$%\^&\*\(\)–\-\*\'\"@\.`\^\%~]"
    pattern = r"[^A-Za-z0-9_]"
    return re.sub(pattern, "_", s)


def institution_dicts():
    institution_df = pd.read_csv("result/university1.csv", converters={"univ": str,
                                                                       "domain": str,
                                                                       "matched": str,
                                                                       # "rank": str,
                                                                       "number_students": str,
                                                                       "student_staff_ratio": str,
                                                                       "intl_students": str,
                                                                       "female_male_ratio": str})
    institution_df = institution_df.loc[(~institution_df['rank'].isnull())]

    dom2info = {}  # email domain, or domain
    univ2info = {}  # univ

    for i, row in institution_df.iterrows():
        dom = row['domain'].replace('"', '')
        univ = row['univ'].replace('"', '')
        matched = row['matched'].replace('"', '')
        rank = row['rank']
        dom2info[dom] = {"matched": matched, "rank": int(rank)}
        univ2info[univ] = {"matched": matched, "rank": int(rank)}
    # print(dom2info)
    # print(univ2info)
    return dom2info, univ2info


def author_institution_rdf():
    '''
    given a dataframe `author` who has columns `author_id,gender,name,paper_name,emails,dblp,google_scholar,home_page,institution_domain,institution_name`,
    I want to create RDF triples as tuples of three elements, where entities use 'ex:' as prefix.
    If the value of each column is not None, then create the tuples including
    ('ex:'+author_id, 'a', 'ex:Author');
    ('ex:'+author_id, 'ex:gender’, gender);
    ('ex:'+author_id, 'ex:name’, name);
    ('ex:'+author_id, 'ex:email’, email) where email is element of the list emails;
    ('ex:'+author_id, 'ex:dblp’, dblp);
    ('ex:'+author_id, 'ex:google_scholar’, google_scholar);
    ('ex:'+author_id, 'ex:home_page’, home_page);
    ('ex:'+author_id, 'ex:worksIn’, institution_name);
    ('ex:'+institution_name, 'a', 'ex:Institution');
    ('ex:'+institution_name, 'ex:domain', institution_domain).
    Instruction: return the result as list of tuples
    '''

    def convert_emails(e):
        if not e:
            return None
        if '[' in e:
            email_list = eval(e)
            return [e for e in email_list]
        else:
            return [e]
    # institution info
    dom2info, univ2info = institution_dicts()
    author_df = pd.read_csv("result/author.csv", converters={"emails": str,
                                                             "gender": str,
                                                             "name": str,
                                                             "paper_name": str,
                                                             "emails": str,
                                                             "dblp": str,
                                                             "google_scholar": str,
                                                             "home_page": str,
                                                             "institution_domain": str,
                                                             "institution_name": str})
    # author_id,gender,name,paper_name,emails,dblp,google_scholar,home_page,institution_domain,institution_name
    author_df['emails'] = author_df['emails'].apply(lambda x: convert_emails(x))

    def add_by_dom(institution_domain):
        info = dom2info[institution_domain]
        matched = info['matched']
        rank = info['rank']
        # Create an RDF term for the institution name
        institution = ex[special_char_replace(matched.strip().replace(" ", "_"))]
        tuples.append((institution, rdflib.RDF.type, ex.Institution))
        tuples.append((institution, ex.name, rdflib.Literal(matched)))
        tuples.append((institution, ex.world_rank, rdflib.Literal(rank)))
        tuples.append((author, ex.worksIn, institution))
        tuples.append((institution, ex.domain, rdflib.Literal(institution_domain)))

    def add_by_univ_name(institution_name):
        info = univ2info[institution_name]
        matched = info['matched']
        rank = info['rank']
        # Create an RDF term for the institution name
        institution = ex[special_char_replace(matched.strip().replace(" ", "_"))]
        tuples.append((institution, ex.name, rdflib.Literal(matched)))
        tuples.append((institution, rdflib.RDF.type, ex.Institution))
        tuples.append((institution, ex.world_rank, rdflib.Literal(rank)))
        tuples.append((author, ex.worksIn, institution))

        if institution_domain:
            tuples.append((institution, ex.domain, rdflib.Literal(institution_domain)))

    tuples = []

    # Loop through each row of the dataframe
    for index, row in author_df.iterrows():
        # Get the values of each column
        author_id = special_char_replace(row["author_id"])
        gender = row["gender"].replace('"', '')
        name = row["name"].replace('"', '')
        paper_name = row["paper_name"].replace('"', '')
        emails = row["emails"]
        dblp = row["dblp"].replace('"', '')
        google_scholar = row["google_scholar"].replace('"', '')
        home_page = row["home_page"].replace('"', '')
        institution_domain = row["institution_domain"].replace('"', '')
        institution_name = row["institution_name"].replace('"', '')

        # Create an RDF term for the author id
        author = ex[author_id]

        # Add the triples for the author id and its properties
        tuples.append((author, rdflib.RDF.type, ex.Author))
        if gender:
            tuples.append((author, ex.gender, rdflib.Literal(gender)))
        if name:
            tuples.append((author, ex.name, rdflib.Literal(name)))
        if emails:
            for email in emails:
                if "@" not in email:
                    continue
                if "**" not in email:  # valid email
                    tuples.append((author, ex.email, rdflib.Literal(email)))
                email_dom = email.split("@")[1]
                if email_dom in dom2info.keys():
                    add_by_dom(email_dom)      # guess institution of author by their email
                elif ".edu" in email_dom:       # TODO university out of arwu a blank node with rank after 1500 (default)
                    blank = new_blank_node()
                    tuples.append((blank, rdflib.RDF.type, ex.Institution))
                    tuples.append((blank, ex.world_rank, rdflib.Literal(1500)))
                    tuples.append((author, ex.worksIn, blank))
                    tuples.append((blank, ex.domain, rdflib.Literal(email_dom)))
        if dblp:
            tuples.append((author, ex.dblp, rdflib.Literal(dblp)))
        if google_scholar:
            tuples.append((author, ex.google_scholar, rdflib.Literal(google_scholar)))
        if home_page:
            tuples.append((author, ex.home_page, rdflib.Literal(home_page)))
        if institution_name:
            if institution_name in univ2info.keys():
                add_by_univ_name(institution_name)  # guess institution of author by their institution name
        elif institution_domain:
            if institution_domain in dom2info.keys():
                add_by_dom(institution_domain)     # guess institution of author by their institution domain
    return tuples


def paper_authorship_rdf():
    '''
    given a dataframe `paper` who has columns `paper_id,title,abstract,authorids,authors,conference`,
    I want to create RDF triples as tuples of three elements, where entities use 'ex:' as prefix.
    If the value of each column is not None, then create the tuples including
    ('ex:'+paper_id, 'a', 'ex:Paper');
    ('ex:'+paper_id, 'ex:title’, title);
    ('ex:'+paper_id, 'ex:abstract’, abstract);
    ('ex:'+atuhor_id, 'ex:author_of’, 'ex:'+paper_id) where atuhor_id is element of the list authorids;
    ('ex:'+paper_id, 'ex:submitted_to’, 'ex:'+conference);

    paper_id,title,abstract,authorids,authors,conference
    '''
    paper_df = pd.read_csv("result/papers.csv", converters={"paper_id": str,
                                                            "title": str,
                                                            "abstract": str,
                                                            "authorids": eval,
                                                            "authors": eval,
                                                            "conference": str})
    triples = []

    # Loop through each row of the dataframe
    for index, row in paper_df.iterrows():
        # Get the values of each column
        paper_id = special_char_replace(row["paper_id"])
        title = row["title"].replace('"', '')
        abstract = row["abstract"].replace('"', '')
        authorids = row["authorids"]
        conference = row["conference"].replace('"', '')

        # Create an RDF term for the paper id
        paper = ex[paper_id]

        # Add the triples for the paper id and its properties
        triples.append((paper, rdflib.RDF.type, ex.Paper))
        if title:
            triples.append((paper, ex.title, rdflib.Literal(title)))
        if abstract is not None:
            triples.append((paper, ex.abstract, rdflib.Literal(abstract)))
        if conference:
            # Create an RDF term for the conference
            conf = ex[conference]
            # Add the triple for the submitted_to relationship
            triples.append((paper, ex.submitted_to, conf))

        # Loop through each author id in the list
        for author_id in authorids:
            author_id = special_char_replace(author_id)
            # Create an RDF term for the author id
            author = ex[author_id]
            # Add the triple for the author_of relationship
            triples.append((author, ex.author_of, paper))

    # Return the list of triples
    return triples


def conference_rdf():
    '''
    given a dataframe `conference` who has columns `conf_id,blind,accept_rate,submit_count,accept_count,reject_count`,
    I want to create RDF triples as tuples of three elements, where entities use 'ex:' as prefix.
    If the value of each column is not None, then create the tuples including
    ('ex:'+conf_id, 'a', 'ex:Venue');
    ('ex:'+conf_id, 'ex:blind_review_type’, blind);
    ('ex:'+conf_id, 'ex:accept_rate’, accept_rate);
    ('ex:'+conf_id, 'ex:submit_count’, submit_count);
    ('ex:'+conf_id, 'ex:accept_count’, accept_count);
    ('ex:'+conf_id, 'ex:reject_count’, reject_count);

    conf_id,blind,accept_rate,submit_count,accept_count,reject_count
    '''
    confs_df = pd.read_csv("result/conference.csv", converters={"conf_id": str,
                                                                "blind": str,
                                                                "accept_rate": str,
                                                                "submit_count": str,
                                                                "accept_count": str,
                                                                "reject_count": str})

    triples = []

    # Loop through each row of the dataframe
    for index, row in confs_df.iterrows():
        # Get the values of each column
        conf_id = special_char_replace(row["conf_id"])
        blind = row["blind"].replace('"', '')
        accept_rate = row["accept_rate"].replace('"', '')
        submit_count = row["submit_count"].replace('"', '')
        accept_count = row["accept_count"].replace('"', '')
        reject_count = row["reject_count"].replace('"', '')

        # Create an RDF term for the conference id
        conf = ex[conf_id]

        # Add the triples for the conference id and its properties
        triples.append((conf, rdflib.RDF.type, ex.Venue))
        if blind:
            triples.append((conf, ex.blind_review_type, rdflib.Literal(blind)))
        if accept_rate:
            triples.append((conf, ex.accept_rate, rdflib.Literal(float(accept_rate))))
        if submit_count:
            triples.append((conf, ex.submit_count, rdflib.Literal(int(submit_count))))
        if accept_count:
            triples.append((conf, ex.accept_count, rdflib.Literal(int(accept_count))))
        if reject_count:
            triples.append((conf, ex.reject_count, rdflib.Literal(int(reject_count))))

    # Return the list of triples
    return triples


def review_rdf():
    '''
    given a dataframe `conference` who has columns `review_id,paper_id,rating_text,review_title,review,rating`,
    I want to create RDF triples as tuples of three elements, where entities use 'ex:' as prefix.
    If the value of each column is not None, then create the tuples including
    ('ex:'+review_id, 'a', 'ex:Review');
    ('ex:'+review_id, 'ex:review_of’, 'ex:'+paper_id);
    ('ex:'+review_id, 'ex:rating_text’, rating_text);
    ('ex:'+review_id, 'ex:review_title’, review_title);
    ('ex:'+review_id, 'ex:review’, review);
    ('ex:'+review_id, 'ex:rating’, rating);
    review_id,paper_id,rating_text,review_title,review,rating
    '''
    review_df = pd.read_csv("result/review.csv", converters={"review_id": str,
                                                             "paper_id": str,
                                                             "rating_text": str,
                                                             "review_title": str,
                                                             "review": str,
                                                             "rating": str})

    triples = []

    # Loop through each row of the dataframe
    for index, row in review_df.iterrows():
        # Get the values of each column
        review_id = special_char_replace(row["review_id"])
        paper_id = special_char_replace(row["paper_id"])
        rating_text = row["rating_text"].replace('"', '')
        review_title = row["review_title"].replace('"', '')
        review_text = row["review"].replace('"', '')
        rating = row["rating"].replace('"', '')

        # Create an RDF term for the review id
        review = ex[review_id]

        # Add the triples for the review id and its properties
        triples.append((review, rdflib.RDF.type, ex.Review))
        if paper_id:
            # Create an RDF term for the paper id
            paper = ex[paper_id]
            # Add the triple for the review_of relationship
            triples.append((review, ex.review_of, paper))
        if rating_text:
            triples.append((review, ex.rating_text, rdflib.Literal(rating_text)))
        if review_title:
            triples.append((review, ex.review_title, rdflib.Literal(review_title)))
        if review:
            triples.append((review, ex.review, rdflib.Literal(review_text)))
        if rating:
            triples.append((review, ex.rating, rdflib.Literal(float(rating))))

    # Return the list of triples
    return triples


def decision_rdf():
    '''
    given a dataframe `conference` who has columns `decision_id,paper_id,decision_text`,
    I want to create RDF triples as tuples of three elements, where entities use 'ex:' as prefix.
    If the value of each column is not None, then create the tuples including
    ('ex:'+decision_id, 'a', 'ex:Decsion');
    ('ex:'+decision_id, 'ex:decision_of’, 'ex:'+paper_id);
    ('ex:'+decision_id, 'ex:decision’, decision_text);

    decision_id,paper_id,decision_text
    '''
    decision_df = pd.read_csv("result/decision.csv", converters={"decision_id": str,
                                                             "paper_id": str,
                                                             "decision_text": str})

    triples = []

    # Loop through each row of the dataframe
    for index, row in decision_df.iterrows():
        # Get the values of each column
        decision_id = special_char_replace(row["decision_id"])
        paper_id = special_char_replace(row["paper_id"])
        decision_text = row["decision_text"].replace('"', '')

        # Create an RDF term for the decision id
        decision = ex[decision_id]

        # Add the triples for the decision id and its properties
        triples.append((decision, rdflib.RDF.type, ex.Decision))
        if paper_id:
            # Create an RDF term for the paper id
            paper = ex[paper_id]
            # Add the triple for the decision_of relationship
            triples.append((decision, ex.decision_of, paper))
        if decision_text:
            triples.append((decision, ex.decision, rdflib.Literal(decision_text)))

    # Return the list of triples
    return triples


# all_tuples = conference_rdf()
#
# all_tuples = [rdf2tuple(tup) for tup in all_tuples]
# for tup in all_tuples:
#     print(tup)
# exit()

all_tuples = []
all_tuples += author_institution_rdf()
all_tuples += paper_authorship_rdf()
all_tuples += conference_rdf()
all_tuples += review_rdf()
all_tuples += decision_rdf()
all_tuples = [rdf2tuple(tup) for tup in all_tuples]
all_tuples = list(set(all_tuples))


def write_ttl(star=True):
    fn = 'rdf_dataset/open_review{}.ttl'.format("_star" if star else "")
    f = open(fn, 'w', encoding='utf-8')
    prefixes = f"""@prefix ex: <{ex}> .
@prefix rdf: <{namespace.RDF}> .
@prefix rdfs: <{namespace.RDFS}> .
@prefix owl: <{namespace.OWL}> .
@prefix xsd: <{namespace.XSD}> .
    """
    f.write(prefixes + "\n")

    for tup in all_tuples:
        f.write(f"{tup[0]} {tup[1]} {tup[2]} .\n")
    f.close()


write_ttl(True)
write_ttl(False)
