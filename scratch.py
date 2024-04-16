"hdfs://nonpdp01/dev01/publish/bdp017/bdp017_rule_engine/data/atm_rules.json"

    process = "filtration"
    process_key = "query_lookup"
    rule_id = "rule_1"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

    logging.info("\n\n\n *************** MAIN -> FILTRATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "w") as f:
                f.write(query)
                f.write("\n\n")

            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))
            tempDf.createOrReplaceTempView("atm_filtered")

            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)




    # Setting the parameters for identification and calling the rule engine function to get the query_builder.

    process = "identification"
    process_key = "value_lookup"
    rule_id = "rule_2"
    lookup = "true"
    value_key = "check_cheque_transaction"
    table_name = ""
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION VALUE LOOKUP *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:

            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(str(query))
                f.write("\n\n")

            # tempDf = spark.sql(query)
            # logging.info(tempDf.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)

    # Setting the parameters for identification and calling the rule engine function to get the query.

    process = "identification"
    process_key = "query_builder"
    rule_id = "rule_3"
    lookup = "false"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

    logging.info("\n\n\n *************** MAIN -> IDENTIFICATION QUERY BUILDER *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:

            logging.info("valid_parameters and valid rule gen, output is >".format(query))
            with open("output/queries.txt", "a") as f:
                f.write(str(query))
                f.write("\n\n")

            # tempDf2 = spark.sql("select * from atm_filtered  where transaction_total_amount >= 10000")
            tempDf2 = spark.sql(query)
            logging.info(tempDf2.show(truncate=False))
            # tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
        else:
            logging.info(message)
    else:
        logging.info(message)



    process = "identification"
    process_key = "conditional"
    rule_id = "rule_5"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                             lookup,
                                                                             value_key, table_name, dataframes,
                                                                             apply_query)

    logging.info("\n\n\n *************** MAIN -> conditional *********************")


    '''
            ATM PROCESS END TO END 
    
    '''


    process = "filtration"
    process_key = "query_lookup"
    rule_id = "rule_1"
    lookup = "true"
    value_key = ""
    table_name = "atm_transactions"
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> FILTRATION *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("\n\n Valid Parameters and Valid rule gen, output is > {}".format(query))
            with open("output/queries.txt", "w") as f:
                f.write(str(query))
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))
                output_df.createOrReplaceTempView("atm_filtered")

            '''
            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))
            tempDf.createOrReplaceTempView("atm_filtered")

            tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
            '''
        else:
            logging.info(message)
    else:
        logging.info(message)





    # Setting the parameters for filtration and calling the rule engine function to get the lookup query.

    process = "identification"
    process_key = "conditional"
    rule_id = "rule_5"
    lookup = "true"
    value_key = ""
    table_name = "atm_filtered"
    dataframes = [atm]
    apply_query = True
    valid_parameters, valid_rule_gen, message, query, output_df = rule_generator(spark, process, process_key, rule_id,
                                                                                 lookup,
                                                                                 value_key, table_name, dataframes,
                                                                                 apply_query)

    logging.info("\n\n\n *************** MAIN -> conditional *********************")
    logging.info("valid_parameters > {}".format(valid_parameters))
    logging.info("valid_rule_gen   > {}".format(valid_rule_gen))
    logging.info("message          > {}".format(message))
    logging.info("query            > {}".format(query))

    if valid_parameters:
        if valid_rule_gen:
            logging.info("\n\n Valid Parameters and Valid rule gen, output is > {}".format(query))
            with open("output/queries.txt", "w") as f:
                f.write(str(query))
                f.write("\n\n")

            if apply_query:
                logging.info(output_df.show(truncate=False))

            '''
            tempDf = spark.sql(query)
            logging.info(tempDf.show(truncate=False))
            tempDf.createOrReplaceTempView("atm_filtered")

            tempDf.repartition(1).write.option("header", "true").csv("output/Dataframe")
            '''
        else:
            logging.info(message)
    else:
        logging.info(message)