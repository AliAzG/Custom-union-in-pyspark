# ---------------------------------------- Functions For Custom Union, Use unionX ----------------------------------------------- #
def __orderDFAndAddMissingCols(df, columnsOrderList, dfMissingFields):
    if not dfMissingFields:
        return df.select(columnsOrderList)
    else:
        columns = []
        for colName in columnsOrderList:
            if colName not in dfMissingFields:
                columns.append(colName)
            else:
                columns.append(f.lit(None).alias(colName))
        return df.select(columns)


def __addMissingColumns(df, missingColumnNames):

    listMissingColumns = []

    for col in missingColumnNames:
        listMissingColumns.append(f.lit(None).alias(col))

    return df.select(df.schema.names + listMissingColumns)


def __orderAndUnionDFs(leftDF, rightDF, leftListMissCols, rightListMissCols):

    leftDfAllCols = __addMissingColumns(leftDF, leftListMissCols)

    rightDfAllCols = __orderDFAndAddMissingCols(
        rightDF, leftDfAllCols.schema.names, rightListMissCols)

    return leftDfAllCols.union(rightDfAllCols)


def unionX(leftDF, rightDF):

    if leftDF is None:
        raise ValueError('leftDF is null')

    if rightDF is None:
        raise ValueError('rightDF is null')

    if leftDF.schema.names == rightDF.schema.names:
        return leftDF.union(rightDF)

    else:
        leftDFColList = set(leftDF.schema.names)

        rightDFColList = set(rightDF.schema.names)

        rightListMissCols = list(leftDFColList - rightDFColList)

        leftListMissCols = list(rightDFColList - leftDFColList)

        return __orderAndUnionDFs(leftDF, rightDF, leftListMissCols, rightListMissCols)

# ---------------------------------------- Functions For Custom Union, Use unionX ----------------------------------------------- #

