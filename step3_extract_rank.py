import pandas as pd
import numpy as np

url_stats = 'https://www.timeshighereducation.com/world-university-rankings/2022/world-ranking#!/page/0/length/-1/sort_by/rank/sort_order/asc/cols/stats'
# url_scores = 'https://www.timeshighereducation.com/world-university-rankings/2022/world-ranking#!/page/0/length/-1/sort_by/rank/sort_order/asc/cols/scores'

'''  
######################################################################################################################################
# open the url_stats in browser, press F12, select the root element, right click mouse, save the element, then past it into arwu.html
######################################################################################################################################
'''

from bs4 import BeautifulSoup
url = "result/arwu2022.html"
stats_page_soup = BeautifulSoup(open(url, encoding='utf-8'), "html.parser")

# collect HTML objects
rank_obj = stats_page_soup.findAll("td", {"class":"rank sorting_1 sorting_2"})
print(len(rank_obj))

names_obj = stats_page_soup.findAll("a", {"class": "ranking-institution-title"})
print(len(names_obj))

stats_number_students_obj = stats_page_soup.findAll("td", {"class": "stats stats_number_students"})
stats_student_staff_ratio_obj = stats_page_soup.findAll("td", {"class":"stats stats_student_staff_ratio"})
stats_pc_intl_students_obj = stats_page_soup.findAll("td", {"class":"stats stats_pc_intl_students"})
stats_female_male_ratio_obj = stats_page_soup.findAll("td", {"class":"stats stats_female_male_ratio"})


rank, names, number_students, student_staff_ratio, intl_students, female_male_ratio, web_address = [], [], [], [], [], [], []
for i in range(len(names_obj)):
    rank.append(rank_obj[i].text)
    names.append(names_obj[i].text)
    number_students.append(stats_number_students_obj[i].text)
    student_staff_ratio.append(stats_student_staff_ratio_obj[i].text)
    intl_students.append(stats_pc_intl_students_obj[i].text)
    female_male_ratio.append(stats_female_male_ratio_obj[i].text[:2])


df = pd.DataFrame({
    'rank' : rank,
    'name' : names,
    'number_students' : number_students,
    'student_staff_ratio' : student_staff_ratio,
    'intl_students' : intl_students,
    'female_male_ratio' : female_male_ratio,
})


def transfer_rank(x):
    if "=" in x:
        return int(x[1:])
    elif "+" in x:
        return int(x[:-1])
    elif "Reporter" in x:
        return 1202
    elif "–" in x:
        return int(np.mean([int(e) for e in x.split("–")]))
    else:
        return int(x)

df['rank'] = df['rank'].apply(lambda x: transfer_rank(str(x)))
df.to_csv("result/world_rank2022.csv", index=False)











'''

import pandas as pd
from bs4 import BeautifulSoup as soup
from selenium import webdriver


url_stats = 'https://www.timeshighereducation.com/world-university-rankings/2022/world-ranking#!/page/0/length/-1/sort_by/rank/sort_order/asc/cols/stats'
# url_scores = 'https://www.timeshighereducation.com/world-university-rankings/2022/world-ranking#!/page/0/length/-1/sort_by/rank/sort_order/asc/cols/scores'

stats_browser = webdriver.Chrome()
# scores_browser = webdriver.Chrome()
#
# # use the webdriver to request the ranking webpage
stats_browser.get(url_stats)
#
# # collect the webpage HTML after its loading
stats_page_html = stats_browser.page_source
# 
# # parse the HTML using BeautifulSoup
stats_page_soup = soup(stats_page_html, 'html.parser')


# collect HTML objects
rank_obj = stats_page_soup.findAll("td", {"class":"rank sorting_1 sorting_2"})
print(len(rank_obj))

names_obj = stats_page_soup.findAll("a", {"class": "ranking-institution-title"})
print(len(names_obj))

stats_number_students_obj = stats_page_soup.findAll("td", {"class": "stats stats_number_students"})
stats_student_staff_ratio_obj = stats_page_soup.findAll("td", {"class":"stats stats_student_staff_ratio"})
stats_pc_intl_students_obj = stats_page_soup.findAll("td", {"class":"stats stats_pc_intl_students"})
stats_female_male_ratio_obj = stats_page_soup.findAll("td", {"class":"stats stats_female_male_ratio"})

# close the browser
stats_browser.close()




# use the webdriver to request the scores webpage
# scores_browser.get(url_scores)

# collect the webpage HTML after its loading
# scores_page_html = scores_browser.page_source
# scores_page_soup = soup(scores_page_html, 'html.parser')
#
# # parse the HTML using BeautifulSoup
# overall_score_obj = scores_page_soup.findAll("td", {"class":"scores overall-score"})
# teaching_score_obj = scores_page_soup.findAll("td", {"class":"scores teaching-score"})
# research_score_obj = scores_page_soup.findAll("td", {"class":"scores research-score"})
# citations_score_obj = scores_page_soup.findAll("td", {"class":"scores citations-score"})
# industry_income_score_obj = scores_page_soup.findAll("td", {"class":"scores industry_income-score"})
# international_outlook_score_obj = scores_page_soup.findAll("td", {"class":"scores international_outlook-score"})
#
# # close the browser
# scores_browser.close()

rank, names, number_students, student_staff_ratio, intl_students, female_male_ratio, web_address = [], [], [], [], [], [], []
# overall_score, teaching_score, research_score, citations_score, industry_income_score, international_outlook_score = [], [], [], [], [], []
for i in range(len(names_obj)):
    # web_address.append('https://www.timeshighereducation.com' + names_obj[i].a.get('href'))
    # print(rank_obj[i].text, names_obj[i].text)

    rank.append(rank_obj[i].text)
    names.append(names_obj[i].text)
    number_students.append(stats_number_students_obj[i].text)
    student_staff_ratio.append(stats_student_staff_ratio_obj[i].text)
    intl_students.append(stats_pc_intl_students_obj[i].text)
    female_male_ratio.append(stats_female_male_ratio_obj[i].text[:2])

    # overall_score.append(overall_score_obj[i].text)
    # teaching_score.append(teaching_score_obj[i].text)
    # research_score.append(research_score_obj[i].text)
    # citations_score.append(citations_score_obj[i].text)
    # industry_income_score.append(industry_income_score_obj[i].text)
    # international_outlook_score.append(international_outlook_score_obj[i].text)


# full_address_list, streetAddress_list, addressLocality_list, addressRegion_list, postalCode_list, addressCountry_list  = [], [], [], [], [], []
# for web in web_address:
#     page = urlopen(web)
#     page_html = soup(page, 'html.parser')
#     location = page_html.findAll('script', {'type':"application/ld+json"})
#     jt = json.loads(location[0].text)
#     jsonnn_tree = objectpath.Tree(jt)
#     streetAddress_list.append(list(jsonnn_tree.execute('$..streetAddress'))[0])
#     addressLocality_list.append(list(jsonnn_tree.execute('$..addressLocality'))[0])
#     addressRegion_list.append(list(jsonnn_tree.execute('$..addressRegion'))[0])
#     postalCode_list.append(list(jsonnn_tree.execute('$..postalCode'))[0])
#     addressCountry_list.append(list(jsonnn_tree.execute('$..addressCountry'))[0])
#     full_address = page_html.findAll('div', {'class':"institution-info__contact-detail institution-info__contact-detail--address"})[0].text.strip()
#     full_address_list.append(full_address)
#     print ('{} out of {}'.format(len(full_address_list), len (web_address)), full_address)



df = pd.DataFrame({
    'rank' : rank,
    'name' : names,
    'number_students' : number_students,
    'student_staff_ratio' : student_staff_ratio,
    'intl_students' : intl_students,
    'female_male_ratio' : female_male_ratio,
    # 'overall_score' : overall_score,
    # 'teaching_score' : teaching_score,
    # 'research_score' : research_score,
    # 'citations_score' : citations_score,
    # 'industry_income_score' : industry_income_score,
    # 'international_outlook_score' : international_outlook_score,
    # 'address' : full_address_list,
    # 'street_address' : streetAddress_list,
    # 'locality_address' : addressLocality_list,
    # 'region_address' : addressRegion_list,
    # 'postcode_address' : postalCode_list,
    # 'country_address' : addressCountry_list
})
df.to_csv("world_rank.csv", index=False)














'''






