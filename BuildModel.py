import csv
import sys
import pickle

def ReadData(dictionary, file, maxLines):
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        #next(csv_reader) Skip if there is a header
        for row in csv_reader:
            #print(row)
            line_count += 1
            if maxLines != 0 and line_count > maxLines:
                break

            dictionary[int(row[0])] = [row[1], float(row[3]), float(row[4]), float(row[5]), float(row[6])]
        print(f'Processed {line_count} lines.')

# Removing dates that are not in all assets
def CleanData(Asset1, Asset2):
    Asset1Table = []
    Asset2Table = []

    for key in sorted(Asset1.keys()): 
        #print(Asset1[key])
        if key in Asset2:
            #print(key)
            Asset1Table.append(Asset1[key])
            Asset2Table.append(Asset2[key])
        else:
            print( "Ignoring", key, Asset1[key] )
    return Asset1Table, Asset2Table

def CleanData3(Asset1, Asset2, Asset3, limitTS):
    Asset1Table = []
    Asset2Table = []
    Asset3Table = []

    for key in sorted(Asset1.keys()): 

        #print(Asset1[key])
        if (int(key) < limitTS) and key in Asset2 and key in Asset3:
            #print(key)
            Asset1Table.append(Asset1[key])
            Asset2Table.append(Asset2[key])
            Asset3Table.append(Asset3[key])
        else:
            print( "Ignoring", key, Asset1[key] )
    return Asset1Table, Asset2Table, Asset3Table

def GetDiv( retOC, divisions ):
    for div in range( 0, len(divisions)):
        rangeTemp = divisions[div]
        rangeMin = rangeTemp[1]
        rangeMax = rangeTemp[2]
        if retOC >= rangeMin and retOC <= rangeMax:
            return div
    assert(False)

def BuildAssetModel(Assets, numPeriods, divisions, filename):
    assert( numPeriods >= 1 )
    dictionaryOcurrences = {}
    dictionaryDistributions = {}
    totalAssets = len(Assets)
    assert( totalAssets > 0 )
    if totalAssets>0:
        if totalAssets >= 2:
            assert( len(Assets[0][1]) == len(Assets[1][1]))
        if totalAssets >= 3:
            assert( len(Assets[1][1]) == len(Assets[2][1]))
    numEntries = len(Assets[0][1])
    firstBar = numEntries-1
    lastBar = firstBar - numEntries + numPeriods
    
    for step in range(0,2): # We want the numPeriods combinations and numPeriods-1
        for bar in range(firstBar,lastBar-1,-1):
            #print( "bar", bar )
            strTotalPeriods = ""
            for period in range(0,numPeriods-step):
                periodAssetsMoves = ""
                for asset in Assets:

                    #calculate return
                    opn  = asset[1][bar-period][1]
                    hig  = asset[1][bar-period][2]
                    low  = asset[1][bar-period][3]
                    clo  = asset[1][bar-period][4]

                    retOC = (float(clo)/float(opn)-1)*100.0
                    #print( asset[0], retOC )

                    for div in range( 0, len(divisions)):
                        rangeTemp = divisions[div]
                        rangeName = rangeTemp[0]
                        rangeMin = rangeTemp[1]
                        rangeMax = rangeTemp[2]
                        if retOC >= rangeMin and retOC <= rangeMax:
                            #print( rangeName, retOC)
                            break
                    assetMove = asset[0]+rangeName
                    periodAssetsMoves += assetMove
                if len( strTotalPeriods) > 0:
                    strTotalPeriods += '+' 
                strTotalPeriods += periodAssetsMoves 

            if strTotalPeriods in dictionaryOcurrences:
                dictionaryOcurrences[strTotalPeriods] = dictionaryOcurrences[strTotalPeriods] + 1
            else:
                dictionaryOcurrences[strTotalPeriods] = 1
        
    print( dictionaryOcurrences )

    #Distributions
    print( "Distributions: ")
    '''
    prepare list of single component
    prepare list of roots (numperiods-1 singlecomponents jointed)
    '''
    allAssetsPlusdivision = []
    for asset in Assets:
        assetPlusDivision = []
        assetName = asset[0]
        print( assetName)
        for div in divisions:
            strTemp = assetName + div[0]
            assetPlusDivision.append(strTemp)
        allAssetsPlusdivision.append(assetPlusDivision)

    stringTemp = ""
    singleComponents = []

    getSingleComponents( singleComponents, allAssetsPlusdivision, stringTemp, 0 )

    #multipleComponentsPeriods = []
    multipleComponentsPeriodsMinusOne = []
    #getMultipleComponents( multipleComponentsPeriods, singleComponents, "", numPeriods, 0 )
    getMultipleComponents( multipleComponentsPeriodsMinusOne, singleComponents, "", numPeriods-1, 0 )

    for elem in multipleComponentsPeriodsMinusOne:
        elemTimes = 0
        ocurrencesAllAssets = []
        for asset in allAssetsPlusdivision:
            if elem in dictionaryOcurrences:
                elemTimes = dictionaryOcurrences[elem]
            
            occurrences = []
            for division in asset:
                occurrencesInDivision = 0
                if elemTimes > 0:
                    singleComponentsInDivision = []
                    stringTemp = ""
                    getSingleComponentsExceptOneType( singleComponentsInDivision, allAssetsPlusdivision, stringTemp, division, 0 )
                    for subdiv in singleComponentsInDivision:
                        temp = elem+'+'+subdiv
                        if temp in dictionaryOcurrences:
                            occurrencesInDivision += dictionaryOcurrences[temp]

                    occurrences.append(occurrencesInDivision/elemTimes)
                else:
                    occurrences.append(0)
            ocurrencesAllAssets.append(occurrences)
        if elemTimes > 0:
            assert( elem not in dictionaryDistributions)
            dictionaryDistributions[elem] = ocurrencesAllAssets

    outfile = open(filename+"_ocurrences",'wb')
    pickle.dump(dictionaryOcurrences,outfile)
    outfile.close()

    outfile = open(filename+"_distributions",'wb')
    pickle.dump(dictionaryDistributions,outfile)
    outfile.close()

    return dictionaryOcurrences, dictionaryDistributions

def getSingleComponents( singleComponents, allAssetsPlusdivision, stringTempParent, asset ):

    assetDiv = allAssetsPlusdivision[asset]
    for elem in assetDiv:
        stringTemp = stringTempParent + elem
        if asset == (len(allAssetsPlusdivision)-1):
            singleComponents.append( stringTemp )
        else:
            getSingleComponents( singleComponents, allAssetsPlusdivision, stringTemp, asset+1)
                
    return

def getSingleComponentsExceptOneType( singleComponents, allAssetsPlusdivision, stringTempParent, exceptionAssetPlusDiv, asset ):

    assetDiv = allAssetsPlusdivision[asset]

    if exceptionAssetPlusDiv in assetDiv:
        stringTemp = stringTempParent + exceptionAssetPlusDiv
        if asset == (len(allAssetsPlusdivision)-1):
            singleComponents.append( stringTemp )
        else:
            getSingleComponentsExceptOneType( singleComponents, allAssetsPlusdivision, stringTemp, exceptionAssetPlusDiv, asset+1)
    else:
        for elem in assetDiv:
            stringTemp = stringTempParent + elem
            if asset == (len(allAssetsPlusdivision)-1):
                singleComponents.append( stringTemp )
            else:
                getSingleComponentsExceptOneType( singleComponents, allAssetsPlusdivision, stringTemp, exceptionAssetPlusDiv, asset+1)
                
    return

def getMultipleComponents( multipleComponentsPeriods, singleComponents, strParent, numPeriods, currentPeriod ):
    for elem in singleComponents:
        strTemp = strParent+elem
        if currentPeriod == (numPeriods-1):
            multipleComponentsPeriods.append(strTemp)
        else:
            getMultipleComponents( multipleComponentsPeriods, singleComponents, strTemp+"+", numPeriods, currentPeriod+1)
    return

def main():
    BTC = {}
    ReadData(BTC, 'gemini_BTCUSD_1min.csv', 0)
    ETH = {}
    ReadData(ETH, 'gemini_ETHUSD_1min.csv', 0)
    LTC = {}
    ReadData(LTC, 'gemini_LTCUSD_1min.csv', 0)

    tsLimit = 1609459140000 + 1 #December 31st
    BTCTable, ETHTable, LTCTable = CleanData3(BTC,ETH, LTC, tsLimit )

    divisions3  = [
        ["down", -1000, -0.01],
        ["flat", -0.01, 0.01],
        ["up", 0.01, 1000]
     ]
    
    divisions5  = [
        ["bigdown", -1000, -1.00],
        ["down", -1, -0.01],
        ["flat", -0.01, 0.01],
        ["up", 0.01, 1],
        ["bigup", 1, 1000]
     ]
    
    divisions9  = [
        ["bigdown", -1000, -0.45],
        ["down3", -0.45, -0.30],
        ["down2", -0.30, -0.15],
        ["down1", -0.15, -0.01],
        ["flat", -0.01, 0.01],
        ["up1", 0.01, 0.15],
        ["up2", 0.15, 0.30],
        ["up3", 0.30, 0.45],
        ["bigup", 0.45, 1000]
     ]

    divisions = {}
    divisions[3] = divisions3
    divisions[5] = divisions5
    divisions[9] = divisions9
    nModel = -1
    for numAssets in [1,2,3]:
        str_num_assets = str(numAssets)
        
        for numbars in [100000,200000,300000,400000,500000]:
            assetsTotal = [["BTC",BTCTable[-numbars:]],["ETH",ETHTable[-numbars:]],["LTC",LTCTable[-numbars:]]]
            assets = assetsTotal[0:numAssets]
        
            for div in [3,5,9]:
                str_num_divisions = str(div)
                
                for lookup_periods in [2,3]:
                    str_lookup_periods = str(lookup_periods)
                    
                    dictionaryOcurrences, dictionaryDistributions =\
                        BuildAssetModel(assets, lookup_periods, divisions[div], str_num_assets+"Assets_"+str_lookup_periods+"Periods_"+str_num_divisions+"Divisions_"+str(round(numbars/1000))+"K" )
                    
                    nModel += 1
                    print( str(nModel) + ":", numAssets, numbars, div, lookup_periods)                        

main()