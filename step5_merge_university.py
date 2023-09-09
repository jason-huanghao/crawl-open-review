import pandas as pd
from domain_univ_tool import find_university_by_domain
import difflib


arwu_df = pd.read_csv("result/world_rank2022.csv", converters={"name": str})
univ_df = pd.read_csv("result/university.csv", converters={"univ": str})


def get_univ_sub_domain(domain: str):
    if not domain:
        return None
    if "edu" not in domain or "." not in domain:
        return None
    domain = domain[domain.index(".")+1:]
    if domain.startswith("edu"):
        return None
    # student.adelaide.edu.au, neurotheory.columbia.edu
    return find_university_by_domain(domain)


univ_df['univ'] = univ_df.apply(lambda x: x['univ'] if x['univ'] else get_univ_sub_domain(x['domain']), axis=1)


mathced = []
rank = []
number_students = []
student_staff_ratio = []
intl_students = []
female_male_ratio = []


def add_col_vals(m, r, n, s, i, f):
    mathced.append(m)
    rank.append(r)
    number_students.append(n)
    student_staff_ratio.append(s)
    intl_students.append(i)
    female_male_ratio.append(f)


all_univ_name = arwu_df['name'].tolist()
arwu_df.set_index('name', inplace=True)

for i, row in univ_df.iterrows():
    if not row['univ']:
        add_col_vals(None, None, None, None, None, None)
        continue

    matches = difflib.get_close_matches(row['univ'], all_univ_name, n=1, cutoff=0.7)
    if not matches:
        add_col_vals(None, None, None, None, None, None)
        continue

    add_col_vals(matches[0],
                 arwu_df.loc[matches[0], 'rank'],
                 arwu_df.loc[matches[0], 'number_students'],
                 arwu_df.loc[matches[0], 'student_staff_ratio'],
                 arwu_df.loc[matches[0], 'intl_students'],
                 arwu_df.loc[matches[0], 'female_male_ratio'])

univ_df['matched'] = mathced
univ_df['rank'] = rank
univ_df['number_students'] = number_students
univ_df['student_staff_ratio'] = number_students
univ_df['intl_students'] = intl_students
univ_df['female_male_ratio'] = female_male_ratio


univ_df.to_csv("result/university1.csv", index=False)





# print(difflib.get_close_matches('Ruprecht-Karls-Universität Heidelberg', ['Universität Heidelberg', 'Karolinska Institute', 'University of Washington'], n=1, cutoff=0.7))

