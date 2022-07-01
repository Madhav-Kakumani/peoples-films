import requests
import json
import mysql.connector


def extract_people(url: str) -> tuple:
    """
    Extract peoples data by making API call based on the given url
    :param url: API to extract peoples data
    :return: url for next page and peoples data returned from API
    """
    response = requests.get(f"{url}")
    if response.status_code != 200:
        message = f"API returned http status code {response.status_code} and error {response.text} for url {url}"
        raise Exception(message)
    else:
        data = response.content
        info = json.loads(data)
        next_url = info['next']

    return next_url, info


def load_people_data(people_list: list) -> bool:
    """
    Load all the people returned from the API into mysql table
    :param people_list: List of all the people across all the pages
    :return: if load is successful returning 'True'
    """
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='people',
                                             user='root',
                                             password='Madhavarao1')

        mycursor = connection.cursor()

        mycursor.execute("""CREATE TABLE if not exists people 
                        (name VARCHAR(255), birth_year VARCHAR(255), 
                        eye_color VARCHAR(255), gender VARCHAR(255), 
                        hair_color VARCHAR(255), height VARCHAR(255), 
                        mass VARCHAR(255), skin_color VARCHAR(255), 
                        homeworld VARCHAR(255), films JSON, species JSON, 
                        starships JSON, vehicles JSON, url VARCHAR(255), 
                        created VARCHAR(255), edited VARCHAR(255))""")

        mycursor.execute("""truncate table people""")

        for pdata in people_list:
            film_json = json.dumps(pdata["films"])
            species_json = json.dumps(pdata["species"])
            starships_json = json.dumps(pdata["starships"])
            vehicles_json = json.dumps(pdata["vehicles"])

            mycursor.execute("""INSERT INTO people
                            (name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld,
                            films, species, starships, vehicles, url, created, edited)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (pdata["name"], pdata["birth_year"], pdata["eye_color"], pdata["gender"],
                            pdata["hair_color"], pdata["height"], pdata["mass"], pdata["skin_color"],
                            pdata["homeworld"], film_json, species_json, starships_json,
                            vehicles_json, pdata["url"], pdata["created"], pdata["edited"]))

        connection.commit()
        load_success = 'True'
        mycursor.close()
        connection.close()

    except Exception as e:
        print("Error while connecting to MySQL to load raw peoples data", e)
        raise e

    return load_success


def transform_aggregate_people() -> None:
    """
    Transform and aggregate peoples data to find oldest character in each film
    :return:
    """
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='people',
                                             user='root',
                                             password='Madhavarao1')

        mycursor = connection.cursor()
        mycursor.execute("""create table if not exists people_type_data 
                            (name varchar(255),
                            birth_year varchar(255),
                            films varchar(255))""")

        mycursor.execute("""create table if not exists people_transform_data
                            (name varchar(255),
                            birth_year float,
                            birth_year_battle varchar(255),
                            films varchar(255))""")

        mycursor.execute("""create table if not exists people_film_birth_agg
                            (films varchar(255),
                            min_birth_year float,
                            birth_year_battle varchar(255))""")

        mycursor.execute("""create table if not exists old_character_by_film
                            (name varchar(255),
                            films varchar(255),
                            birth_year float,
                            birth_year_battle varchar(255))""")

        mycursor.execute("""truncate table people_type_data""")
        mycursor.execute("""truncate table people_transform_data""")
        mycursor.execute("""truncate table people_film_birth_agg""")
        mycursor.execute("""truncate table old_character_by_film""")

        mycursor.execute("""insert into people_type_data
                            (select name, birth_year, films from people where birth_year != 'unknown')""")

        mycursor.execute("""insert into people_transform_data
                            (select p.name,
                            CASE
                            WHEN p.birth_year LIKE '%BBY' THEN REPLACE(p.birth_year, 'BBY', "")
                            WHEN p.birth_year LIKE '%ABY' THEN REPLACE(p.birth_year, 'ABY', "")
                            ELSE ''
                            END as birth_year,
                            CASE
                            WHEN p.birth_year LIKE '%BBY' THEN 'BBY'
                            WHEN p.birth_year LIKE '%ABY' THEN 'ABY'
                            ELSE ''
                            END AS birth_year_battle,
                            TRIM(REPLACE(REPLACE(REPLACE(j.films, '[', ""), ']', ""), '"', "")) as films
                            from people_type_data as p
                            join json_table(
                                replace(json_array(p.films), ',', '","'),
                                '$[*]' columns (films varchar(50) path '$')
                            ) j)""")

        mycursor.execute("""insert into people_film_birth_agg
                            (select films,
                            min(birth_year) as min_birth_year,
                            birth_year_battle 
                            from people_transform_data
                            group by films, birth_year_battle)""")

        mycursor.execute("""insert into old_character_by_film
                            (select pt.name,
                            pt.films,
                            pt.birth_year,
                            ba.birth_year_battle
                            from people_transform_data as pt, people_film_birth_agg as ba
                            where pt.films = ba.films
                            and pt.birth_year = ba.min_birth_year
                            and ba.birth_year_battle = 'BBY'
                            UNION
                            select pt.name,
                            pt.films,
                            pt.birth_year,
                            ba.birth_year_battle
                            from people_transform_data as pt, people_film_birth_agg as ba
                            where pt.films = ba.films
                            and pt.birth_year = ba.min_birth_year
                            and ba.birth_year_battle = 'ABY')""")


        connection.commit()
        mycursor.close()
        connection.close()

    except Exception as e:
        print("Error while connecting to MySQL to transform and aggregate peoples data", e)
        raise e


def call_api() -> list:
    """
    Make a list of all the people returned from the API
    :return: contains list of all the people
    """
    url = 'https://swapi.dev/api/people'
    next_url, info = extract_people(url)
    people_data = [i for i in info['results']]
    while next_url:
        next_url, info = extract_people(next_url)
        if next_url is None:
            break
        else:
            people_ndata = [i for i in info['results']]
            people_data.extend(people_ndata)

    return people_data
