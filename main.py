import munging

def main():
    #only use this once to initiallize into the database uncomment if the database has not been in modified before
    #munging.initialize()

    moded_data = munging.munging()

    #only use this once to finalize the new database uncomment if the database has not been in modified before
    #munging.finalize(moded_data)

    munging.check_db()

if __name__ == "__main__":
    main()
