import time
from time import sleep
from warnings import warn
import urllib.parse as up
from requests import get
from bs4 import BeautifulSoup
import re
import psycopg2


def resultsScraper(countyCode, precinctCodes):
    precinct_entry = ""

    precincts = ""
    for precinct in precinctCodes:
        precincts += precinct
        precincts += ","
    precincts = precincts[:-1]

    # specify the url with 'countyid' and 'precincts'
    response = get(
        'https://electionresults.sos.state.mn.us/Results/PrecinctListResults/115?countyid=' + countyCode + '&precincts=' + precincts)

    # parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(response.text, 'html.parser')

    # Selects starting area in html at 'center'
    center = soup.find('center')

    # Creates list of precinct names,
    # sets iterator at 1 to skip "Results for Selected Precincts in Hennepin County"
    precinct_containers = soup.find_all('div', class_='resultgroupheader')
    pnum = 1

    # Creates list of all tables which is where results are stored for each precinct
    tables = center.find_all('table')

    # Iterates through table
    for ptable in tables:

        # Holds the name of the office_name candidates are running for i.e. U.S. Senator
        office_name = ""

        # Creates list of all rows which is where each candidates results are stored
        rows = ptable.find_all('tr')

        # Iterates through candidates
        for row in rows:

            # Initializes the string that holds the row for each candidate result in table
            # with precinct name and office name
            rowentry = "('" + precinct_containers[0].text.strip()[34:-7].replace("'", "") + "','" + precinct_containers[
                pnum].text.strip().replace("'", "") + "','" + office_name.replace("'", "") + "'"

            # Check if the row has 'class' so it doesn't error, skips if doesn't
            if row.has_attr('class'):

                # Updates the 'office_name' variable to whichever seat candidates are running for
                if row['class'] == ['resultofficeheader']:

                    # Generates and cleans the office name
                    office_name = row.find('div', class_='resultoffice')
                    office_name = office_name.text.strip()
                    office_name = re.sub(r"\s+", " ", office_name)

                # If not a new office, check if a candidate result
                elif row['class'] == ['resultcandidates']:

                    # Selects appropriate entries, cleans extra empty field, cleans text
                    cols = row.find_all('td')[:4]
                    cols = [ele.text.strip() for ele in cols]
                    if cols:
                        for ele in cols:
                            rowentry += ",'"
                            rowentry += ele.replace("'", "") + "'"
                        rowentry += "),"
                        precinct_entry += rowentry

        # Updates to next precinct once iterated through entire table
        pnum += 1
    return precinct_entry


def precinctCodes(countyCode, reportedPrecincts):
    # List to store codes in
    newPrecincts = []

    # Specificy URL
    response = get('https://electionresults.sos.state.mn.us/Select/CountyPrecinctSelect/115?districtid=' + countyCode)

    # Parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(response.text, 'html.parser')

    # Precinct list
    precinct_list = soup.find_all('option', class_='selOptReported')
    # Check all precinct codes
    for precinct in precinct_list:
        precinctCode = precinct.attrs['value']

        # Compile precincts that reported since last check
        if precinctCode not in reportedPrecincts:
            newPrecincts.append(precinctCode)

        newPrecincts = newPrecincts[:180]
    return newPrecincts


# -- Main -------------------------------------------------------------------------------

conn = psycopg2.connect(dbname="results18", user="dflvictory", password="dflguest18",
                        host="dfl-election-returns.cmycsq7ldygm.us-east-2.rds.amazonaws.com")
cur = conn.cursor()
cur.execute("set time zone 'America/Chicago'")

precinctsReported = [[] for i in range(88)]  ## Make sure this is outside while loop!

while True:
    # precinctsReported = numpy.empty(88, dtype=object)
    # print(precinctsReported)

    # URL
    URL = 'https://electionresults.sos.state.mn.us/Results/CountyStatistics/115'

    # Open URL
    response = get(URL)

    # Monitor Loop
    start_time = time.time()
    requests = 0

    # Throw a warning for non-200 status codes
    if response.status_code != 200:
        requests += 1
        warn('Request: {}; Status code: []'.format(requests, response.status_code))

        # Slow the loop
        sleep(1)

    # Parse the html using beautiful soup and store in variable `soup`
    soup = BeautifulSoup(response.text, 'html.parser')

    # Precinct list
    counties_reported = soup.find_all('tr')

    # Finding County Results
    x = 0
    for county in counties_reported:

        county_entry = "INSERT INTO dry_run5 (county, precinct, office, party, candidate, raw_votes, percentage) values "

        # Removes nulls
        if county.find('a', href=True) is not None:

            x += 1
            print(x)

            # Get County Code
            row = county.find('a', href=True)
            url = row.get('href')
            parsed = up.urlparse(url)
            code = up.parse_qs(parsed.query)['countyId']
            countyCode = code[0]

            # Get Precincts Reported
            reported = county.find('td', class_='statscell statscellnumber').text
            numReported = int(reported)

            # Compared # of precincts currently reported to # previously reported
            if numReported > len(precinctsReported[int(countyCode)]):  # look up syntax for this
                # Call PrecinctCodes helper function
                precinctsUpdated = precinctCodes(countyCode, precinctsReported[int(countyCode)])

                # Call resultsScraper
                if (len(precinctsUpdated) > 0):
                    county_entry += resultsScraper(countyCode, precinctsUpdated)

                # Append new list of precincts to list
                precinctsReported[int(countyCode)] = precinctsReported[int(countyCode)] + precinctsUpdated

                # if county_entry != "INSERT INTO results18 (county, precinct, office, party, candidate, raw_votes, percentage) values ":
                county_entry = county_entry[:-1]
                cur.execute(county_entry)
                conn.commit()
                # else:
                #     print(countyCode)
    print("Ran!")

cur.close()
conn.close()
