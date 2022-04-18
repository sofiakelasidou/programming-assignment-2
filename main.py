import mysql.connector
from mysql.connector import errorcode
from os import getcwd
from csv import reader

from numpy import intp
from sqlalchemy import true


# Connection details
cnx = mysql.connector.connect(user='root',
                             password='root',
                             unix_socket= '/Applications/MAMP/tmp/mysql/mysql.sock')

DB_NAME = 'JoJosBizarreAdventure'

cursor = cnx.cursor()


# Create database "JoJosBizarreAdventure"
def create_database(cursor, DB_NAME):
    try:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Creating database {DB_NAME}")
    except mysql.connector.Error as err:
        print(f"Faild to create database {err}")
        exit(1)


# Read file
def read_file(path):
    lst = []
    with open(path) as file:
        for line in reader(file, delimiter=';'):
            lst.append(line)
    return lst


# Create table "stands"
def create_table_stands(cursor):
    creat_table = "CREATE TABLE `stands` (" \
                 "  `name` varchar(27) NOT NULL," \
                 "  `type` varchar(23)," \
                 "  `power` varchar(9)," \
                 "  `speed` varchar(9)," \
                 "  `range` varchar(9)," \
                 "  `stamina` varchar(9)," \
                 "  `precision` varchar(9)," \
                 "  `potential` varchar(9)," \
                 "  `users_name` varchar(25)," \
                 "  PRIMARY KEY (`name`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table \"stands\"")
        cursor.execute(creat_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table \"stands\" already exists.")
        else:
            print(err.msg)
    else:
        print("DONE")


# Create table "stand_users"
def create_table_stand_users(cursor):
    creat_table = "CREATE TABLE `stand_users` (" \
                 "  `name` varchar(25) NOT NULL," \
                 "  `gender` varchar(6)," \
                 "  `hair_color` varchar(25)," \
                 "  `eye_color` varchar(22)," \
                 "  `status` varchar(8)," \
                 "  PRIMARY KEY (`name`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table \"stand_users\"")
        cursor.execute(creat_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table \"stand_users\" already exists.")
        else:
            print(err.msg)
    else:
        print("DONE")


# Create table "appearances"
def create_table_appearances(cursor):
    creat_table = "CREATE TABLE `appearances` (" \
                 "  `users_name` varchar(25) NOT NULL," \
                 "  `voice_actor` varchar(20)," \
                 "  `role` varchar(16)," \
                 "  `part` TINYINT NOT NULL," \
                 "  `first_appearance` SMALLINT," \
                 "  PRIMARY KEY (`users_name`, `part`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table \"appearances\"")
        cursor.execute(creat_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table \"appearances\" already exists.")
        else:
            print(err.msg)
    else:
        print("DONE")


# Create table "parts"
def create_table_parts(cursor):
    creat_table = "CREATE TABLE `parts` (" \
                 "  `number` TINYINT NOT NULL," \
                 "  `title` varchar(22)," \
                 "  `season` TINYINT," \
                 "  `status` varchar(9)," \
                 "  `year` SMALLINT," \
                 "  `location` varchar(36)," \
                 "  `abilities` varchar(10)," \
                 "  PRIMARY KEY (`number`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table \"parts\"")
        cursor.execute(creat_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table \"parts\" already exists.")
        else:
            print(err.msg)
    else:
        print("DONE")


# Create table "episodes"
def create_table_episodes(cursor):
    creat_table = "CREATE TABLE `episodes` (" \
                 "  `number` SMALLINT NOT NULL," \
                 "  `episode` TINYINT," \
                 "  `part` TINYINT," \
                 "  `title` varchar(54)," \
                 "  `air_date` varchar(9)," \
                 "  PRIMARY KEY (`number`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating table \"episodes\"")
        cursor.execute(creat_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table \"episodes\" already exists.")
        else:
            print(err.msg)
    else:
        print("DONE")


# Insert data into tables
def insert_into_table(cursor, name, data):
    # For each row
    for i in range(1, len(data)):
        # If key columns are not null
        if data[i][0] != "None" and not (name == "appearances" and data[i][3] == "None"):
            # Choose non-null columns of row i
            columns = "`" + data[0][0] + "`"
            row = "\"" + data[i][0] + "\""
            for j in range(1, len(data[0])):
                if data[i][j] != "None":
                    columns += ", `" + data[0][j] + "`"
                    row += ", " + "\"" + data[i][j] + "\""
        # Insert row into table
        insert_sql = [f"INSERT INTO {name} ({columns})"
                      f"VALUES ({row});"
                      ]
        for query in insert_sql:
            try:
                cursor.execute(query)
            except mysql.connector.Error as err:
                print(err.msg)
            else:
                # Make sure data is committed to the database
                cnx.commit()


def print_main_menu():
    print("\n1. List stands and their coresponding users with chosen rankings.\n" \
          "2. Search for stand users from a chosen description.\n" \
          "3. Search for stand users, voice actors, and first episodes they appeared in by chosing a role (and optionally a part).\n" \
          "4. Show most common rankings per category for the different stand types.\n" \
          "5. List details of the parts a given stand or stand user appeared in.\n")


# Use (or create) database "JoJosBizarreAdventure"
try:
    cursor.execute(f"USE {DB_NAME}")
except mysql.connector.Error as err:
    # If database "JoJosBizarreAdventure" does not exist create it
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        print(f"Database {DB_NAME} does not exist")
        create_database(cursor, DB_NAME)
        
        cnx.database = DB_NAME

        # Create table "stands"
        stands = read_file(getcwd() + "/jojo_stands.csv")
        create_table_stands(cursor)
        insert_into_table(cursor, "stands", stands)

        # Create table "stand_users"
        stand_users = read_file(getcwd() + "/jojo_stand_users.csv")
        create_table_stand_users(cursor)
        insert_into_table(cursor, "stand_users", stand_users)

        # Create table "appearances"
        appearances = read_file(getcwd() + "/jojo_appearances.csv")
        create_table_appearances(cursor)
        insert_into_table(cursor, "appearances", appearances)

        # Create table "parts"
        parts = read_file(getcwd() + "/jojo_parts.csv")
        create_table_parts(cursor)
        insert_into_table(cursor, "parts", parts)

        # Create table "episodes"
        episodes = read_file(getcwd() + "/jojo_episodes.csv")
        create_table_episodes(cursor)
        insert_into_table(cursor, "episodes", episodes)

        print(f"Database {DB_NAME} created succesfully.")
        
    # Or print other error
    else:
        print(err)


# Queries

while True:
    print_main_menu()
    q = input("Please chooose one option: ")
    # Query 1
    if q == '1':
        query = '''SELECT `name`, `power`, `speed`, `range`, `stamina`, `precision`, `potential`, `users_name`
                FROM stands WHERE'''
        cnt = 0
        categories = ["power", "speed", "range", "stamina", "precision", "potential"]
        # There are 6 categories in total
        while cnt < 6:
            # Choose category (with correct input check)
            print("\nCategories:\n")
            for i in range(len(categories)):
                print(f"{i + 1} - {categories[i].capitalize()}")
            category = int(input("\nChoose a category: "))
            while category > 6 or category < 1:
                print("\nError: Wrong input!")
                print("\nCategories:\n")
                for i in range(len(categories)):
                    print(f"{i + 1} - {categories[i].capitalize()}")
                category = int(input("\nChoose a category: "))
            # Convert number (from input) into corresponding category
            category = categories[category - 1]
            # Keep track of categories chosen (and their number)
            categories.remove(category)
            cnt += 1
            # Choose rank for chosen category (with correct input check)
            rank = input(f"\nChoose a rank for category \"{category}\" (A/B/C/D/E/I-Infinite): ").upper()
            while not (rank >= 'A' and rank <= 'E' or rank == 'I'):
                print("\nError: Wrong input!")
                rank = input(f"\nChoose a rank for category \"{category}\" (A/B/C/D/E/I-Infinite): ").upper()
            if rank == 'I':
                rank = "Infinite"
            # Add condition to query
            query += f" stands.{category}=\"{rank}\""
            # Ask to choose another condition
            if input("\nWould you like to choose another category (Y/N): ").upper() == 'N':
                cnt = 7
            else:
                query += " and"
        # End and execute query
        query += ";"
        cursor.execute(query)
        # Output
        print("\n{:<27} | {:<8} | {:<8} | {:<8} | {:<8} | {:<9} | {:<9} | {:<25}".format("Stand", "Power", "Speed", "Range", "Stamina", "Precision", "Potential", "Stand User"))
        print("-"*123)
        for stand, power, speed, range, stamina, precision, potential, user in cursor:
            if power == None:
                power = "Unknown"
            if speed == None:
                speed = "Unknown"
            if range == None:
                range = "Unknown"
            if stamina == None:
                stamina = "Unknown"
            if precision == None:
                precision = "Unknown"
            if potential == None:
                potential = "Unknown"
            print("{:<27} | {:<8} | {:<8} | {:<8} | {:<8} | {:<9} | {:<9} | {:<25}".format(stand, power, speed, range, stamina, precision, potential, user))

    # Query 2
    elif q == '2':
        cnt = 0
        query = "SELECT name, gender, hair_color, eye_color, status FROM stand_users WHERE"
        # Choose gender (correct input check)
        if input("\nWould you like to chose a gender? (Y/N): ").upper() == 'Y':
            cnt += 1
            gender = int(input("\nEnter gender (1-male/2-female): "))
            while gender > 2 or gender < 1:
                print("\nError: Wrong input!")
                gender = int(input("\nEnter gender (1-male/2-female): "))
            if gender == 1:
                query += " gender=\"Male\""
            elif gender == 2:
                query += " gender=\"Female\""
        # Choose hair color
        if input("\nWould you like to choose a hair color? (Y/N): ").upper() == 'Y':
            if cnt > 0:
                query += " and"
            cnt += 1
            hair = "\"" + input("\nEnter a hair color: ") + "\""
            query += f" hair_color={hair.lower().capitalize()}"
        # Choose eye color
        if input("\nWould you like to choose an eye color? (Y/N): ").upper() == 'Y':
            if cnt > 0:
                query += " and"
            cnt += 1
            eyes = "\"" + input("\nEnter an eye color: ") + "\""
            query += f" eye_color={eyes.lower().capitalize()}"
        # Choose status (correct input check)
        if input("\nWould you like to choose the characters status? (Y/N): ").upper() == 'Y':
            if cnt > 0:
                query += " and"
            cnt += 1
            status = int(input("\nEnter status (1-Alive/2-Retired/3-Deceased): "))
            while status > 3 or status < 1:
                print("\nError: Wrong input!")
                status = int(input("\nEnter status (1-Alive/2-Retired/3-Deceased): "))
            if status == 1:
                query += " status=\"Alive\""
            elif status == 2:
                query += " status=\"Retired\""
            else:
                query += " status=\"Deceased\""
        # Execute query
        query += ';'
        cursor.execute(query)
        # Output
        print("\n{:<25} | {:<7} | {:<25} | {:<22} | {:<8}".format("Stand User", "Gender", "Hair color", "Eye color", "Status"))
        print("-"*120)
        for stand_user, gender, hair, eyes, status in cursor:
            if gender == None:
                gender = "Unknown"
            if hair == None:
                hair = "Unknown"
            if eyes == None:
                eyes = "Unknown"
            if status == None:
                status = "Unknown"
            print("{:<25} | {:<7} | {:<25} | {:<22} | {:<8}".format(stand_user, gender, hair, eyes, status))

    # Query 3
    elif q == '3':
        print("\nRoles:\n\n1 - Main Protagonist\n2 - Main Ally\n3 - Main Antagonist\n4 - Antagonist\n5 - Minor Antagonist\n6 - Supporting\n7 - Minor Character")
        role = int(input("\nChoose a role: "))
        while role > 7 or role < 1:
            print ("\nError: Wrong input!")
            role = int(input("\nChoose a role: "))
        if role == 1:
            role = "\"" + "Main Protagonist" + "\""
        elif role == 2:
            role = "\"" + "Main Ally" + "\""
        elif role == 3:
            role = "\"" + "Main Antagonist" + "\""
        elif role == 4:
            role = "\"" + "Antagonist" + "\""
        elif role == 5:
            role = "\"" + "Minor Antagonist" + "\""
        elif role == 6:
            role = "\"" + "Supporting" + "\""
        else:
            role = "\"" +  "Minor Character" + "\""
        query = f'''SELECT appearances.users_name, appearances.voice_actor, appearances.role, appearances.part, parts.title, episodes.episode, episodes.title 
                FROM appearances, parts, episodes
                WHERE appearances.role={role}
                and appearances.part=parts.number
                and appearances.first_appearance=episodes.number'''
        if input("\nWould you like to choose a specific part? (Y/N): ").upper() == "Y":
            parts_query = "SELECT number, title FROM parts WHERE number > 0 and number < 7"
            cursor.execute(parts_query)
            print("\nParts:\n")
            for part, name in cursor:
                print(f"{part} - {name}")
            part = int(input("\nChoose a part: "))
            while part > 8 or part < 0:
                print ("Error: Wrong input!")
                part = int(input("\nChoose a part: "))
            query += f" and appearances.part={part}"
        query += '''\nORDER BY appearances.first_appearance;'''
        cursor.execute(query)
        print("\n{:<25} | {:<20} | {:<16} | {:<26} | {:<59}".format("Stand User", "Voice Actor", "Role", "Part", "First Appearance (episode)"))
        print("-"*158)
        for stand_user, voice_actor, role, part_number, part_title, episode_number, episode_title in cursor:
            if voice_actor ==  None:
                voice_actor = "Unknown"
            if role ==  None:
                role = "Unknown"
            print("{:<25} | {:<20} | {:<16} | {} - {:<22} | {:>2} - {:<54}".format(stand_user, voice_actor, role, part_number, part_title, episode_number, episode_title))

    # Query 4
    elif q == '4':
        i = 0
        categories = ["power", "speed", "range", "stamina", "precision", "potential"]
        query = "SELECT `type`, `power`, `speed`, `range`, `stamina`, `precision`, `potential` FROM " + '(' * 4
        for category in categories:
            i += 1
            query += f'''(SELECT DISTINCT `type`, (
            SELECT `{category}`
            FROM stands temp
            WHERE stands.type=temp.type
            GROUP BY temp.{category}
            ORDER BY COUNT(temp.{category}) DESC
            LIMIT 1
            ) as `{category}`
            FROM stands) as t{i}'''
            if i != 1 and i != 6:
                query += f") "
            if i != 6:
                query += "\nNATURAL JOIN\n"
        query += '''
        ORDER BY type;'''
        cursor.execute(query)
        print("\nMost common rankings:")
        print("\n{:<23} | {:<5} | {:<5} | {:<5} | {:<7} | {:<9} | {:<9}".format("Type", "Power", "Speed", "Range", "Stamina", "Precision", "Potential"))
        print("-"*81)
        for type, power, speed, range, stamina, precision, potential in cursor:
            print("{:<23} | {:<5} | {:<5} | {:<5} | {:<7} | {:<9} | {:<9}".format(type, power, speed, range, stamina, precision, potential))

    # Query 5
    elif q == '5':
        # Ask if want to input stand or user (with correct input check)
        inp = int(input("\nWould you like to choose a stand or a stand user? (1-Stand/2-Stand user): "))
        while inp != 1 and inp != 2:
            print("\nError: Wrong input!")
            inp = int(input("\nWould you like to choose a stand or a stand user? (1-Stand/2-Stand user): "))
        # If input is stand name
        if inp == 1:
            stand = "\"" + input("\nEnter a stand name: ").title() + "\""
            query = f'''SELECT parts.number, parts.title, parts.season, parts.status, parts.year, parts.location
                    FROM parts, appearances, stands
                    WHERE stands.name={stand}
                    and stands.users_name=appearances.users_name
                    and appearances.part=parts.number
                    and parts.abilities=\"Stands\";'''
            print(f"\n{stand} has appeared in:")
        # If input is stand user name
        else:
            user = "\"" + input("\nEnter a stand user name: ").title() + "\""
            query = f'''SELECT parts.number, parts.title, parts.season, parts.status, parts.year, parts.location
                    FROM parts, appearances
                    WHERE appearances.users_name={user}
                    and appearances.part=parts.number;'''
            print(f"\n{user} has appeared in:")
        # Execute query and outputs
        cursor.execute(query)
        for part, name, season, status, year, location in cursor:
            print(f"\nPart {part}: {name}\nSeason: {season}", end = ', ')
            if status != "Not Aired":
                print(year)
            else:
                print(status)
            print(f"Location: {location}")

    # Wrong input
    else:
        print("\nError: Wrong input!")

    # Quit or go back to main menu (could not figure out how to do it with any key)
    if input("\nPlease press Enter to continue or Q to quit ").upper() == 'Q':
        break


cursor.close()
cnx.close()
