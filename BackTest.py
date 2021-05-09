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


def main():
    
    BTC = {}
    numbars = 0 # 0 value means we load all data available
    ReadData(BTC, 'gemini_BTCUSD_1min.csv', numbars) #TODO CHANGE DATA NAMES. WE HAVE SOME 2021 DATA.
    ETH = {}
    ReadData(ETH, 'gemini_ETHUSD_1min.csv', numbars)
    LTC = {}
    ReadData(LTC, 'gemini_LTCUSD_1min.csv', numbars)

    AssetsData = [BTC,ETH,LTC]

    tsLimit = 1609459140000 #December 31st
       
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
    divisions = { 3:divisions3,5:divisions5,9:divisions9 }

    #preparing the list of models to Load
    modelsToLoad = []
    OptionsNumAssets = [1,2,3]
    OptionsDiv = [3,5,9]
    OptionsPeriod =  [2,3]
    OptionsNumEntries = [100,200,300,400,500]
        
    for Asset in OptionsNumAssets:
        for Div in OptionsDiv:
            for Period in OptionsPeriod:
                for NumEntries in OptionsNumEntries:
                    modelsToLoad.append([Asset,Div,Period, NumEntries, {}, {}])
        
    #Loading Models (Ocurrences and Distributions)    
    for model in modelsToLoad:
        Asset = str(model[0])
        Div = str(model[1])
        Period = str(model[2])
        NumEntries = str(model[3])
        DictDistributions = {}
        DictOcurrences = {}

        filenameBase = Asset+"Assets_"+Period+"Periods_"+Div+"Divisions_"+NumEntries+"K"
        infile = open(filenameBase+"_distributions",'rb')
        DictDistributions = pickle.load(infile)
        model[4] = DictDistributions
        infile.close()
        infile = open(filenameBase+"_ocurrences",'rb')
        DictOcurrences = pickle.load(infile)
        model[5] = DictOcurrences
        infile.close()

    #Backtesting
    empty3Div =[[0.0,0.0,0.0],[0.0,0.0,0.0],[0.0,0.0,0.0]]
    empty5Div =[[0.0,0.0,0.0,0.0,0.0],[0.0,0.0,0.0,0.0,0.0],[0.0,0.0,0.0,0.0,0.0]]
    empty9Div =[[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0],[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0],[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]]
    empty = {3:empty3Div,5:empty5Div,9:empty9Div}
    cont = True
    currentTS = tsLimit
    numDataPoints = 0
    bars = {}
    for Div in OptionsDiv:
        bars[Div] = []
    
    Pos = [0.0, 0.0, 0.0]
    AvgPrice = [0,0,0]
    AssetNames = ["BTC", "ETH", "LTC"]
    totalPLAsset = [0.0,0.0,0.0]
    totalEarlyClosePl = [0,0,0]
    numLongsTP = [0,0,0]
    numLongsSL = [0,0,0]
    numShortsTP = [0,0,0]
    numShortsSL = [0,0,0]
    numLongsBreakeven = [0,0,0]
    numShortsBreakeven = [0,0,0]
    numLongsEarlyClose = [0,0,0]
    numShortsEarlyClose = [0,0,0]
    sl = [0.0, 0.0, 0.0]
    tp = [0.0, 0.0 ,0.0]
    firstAsset = 0
    lastAsset = 0
    
    epsilon = 0.001

    #Parameters to tweak
    usePLPerc = True
    profitPerc = 0.15/100.0
    lossPerc = 0.075/100.0
    slOffset = 30
    tpOffset = 30
    
    slippage = 1.0
    commision = 1.0
    breakEvenCost = 10

    closingEarly = False

    kProb = 0.66
    kMinOcurrences = 10
    averageModels = False
    reinvest = True
    entryMoneyAmount = 50000

    while cont:
        currentTS += 60000
        
        if currentTS in BTC and currentTS in ETH and currentTS in LTC:
            #print( currentTS )
            numDataPoints += 1
            ret_OC = []
            close = []
            high = []
            low = []
            Var = []

            for Asset in range(0,max(OptionsNumAssets)):
                Var.append(AssetsData[Asset][currentTS])
                close.append(Var[Asset][4])
                high.append(Var[Asset][2])
                low.append(Var[Asset][3])
                ret_OC.append((Var[Asset][4]/Var[Asset][1] - 1.0)*100)
            
            div = []
            for Asset in range(0,max(OptionsNumAssets)):
                div.append({})
                for Div in OptionsDiv:
                    temp = GetDiv( ret_OC[Asset], divisions[Div] )
                    div[Asset][Div] = temp

            for Div in OptionsDiv:
                currentBar = ""
                for Asset in range(0,max(OptionsNumAssets)):
                    strAsset = AssetNames[Asset]
                    stateID = div[Asset][Div]
                    temp =  divisions[Div]
                    stateStr = temp[stateID][0]
                    currentBar += strAsset+stateStr
                bars[Div].append(currentBar)
            
            nbar = len(bars[OptionsDiv[0]])-1
            neededBars = max(OptionsPeriod)
            if len(bars[OptionsDiv[0]]) >= neededBars:
                for assetID in range(firstAsset,lastAsset+1):
                    probabilitiesUp = []
                    probabilitiesDown = []
                    probabilitiesFlat = []
                    for model in modelsToLoad:
                        div = model[1]
                        periods = model[2]
                        dictDistributions = model[4]
                        dictOcurrences = model[5]
                        currentBar = ""
                        for b in range(nbar-(periods-2),nbar+1):
                            if currentBar != "":
                                currentBar += "+"
                            currentBar += bars[div][b]
                        
                        if currentBar in dictDistributions:
                            predictions = dictDistributions[currentBar]
                            num_currences = dictOcurrences[currentBar]
                        else:
                            predictions = empty[div]
                            num_currences = 0

                        probDown = 0.0
                        probUp = 0.0
                        probFlat = 0.0
                        if num_currences >= kMinOcurrences:
                            for d in range(0,int(div/2)):
                                probDown += predictions[assetID][d]
                            for d in range(int(div/2)+1, div):
                                probUp += predictions[assetID][d]
                            probFlat = predictions[assetID][int(div/2)]

                        probabilitiesUp.append( probUp )
                        probabilitiesDown.append( probDown )
                        probabilitiesFlat.append( probFlat )
                    
                    avgProbDown = 0
                    avgProbUp = 0
                    avgProbFlat = 0

                    if averageModels:
                        #probability down
                        cumProbDown = 0
                        numValidPredictionsDown = 0
                        for p in range(0,len(probabilitiesDown)):
                            probTemp = probabilitiesDown[p]
                            if probTemp > 0.01:
                                numValidPredictionsDown += 1
                                cumProbDown += probTemp
                        if numValidPredictionsDown > 0:
                            avgProbDown = cumProbDown/float(numValidPredictionsDown)

                        #probability up
                        
                        cumProbUp = 0
                        numValidPredictionsUp = 0
                        for p in range(0,len(probabilitiesUp)):
                            probTemp = probabilitiesUp[p]
                            if probTemp > 0.01:
                                numValidPredictionsUp += 1
                                cumProbUp += probTemp
                        if numValidPredictionsUp > 0:
                            avgProbUp = cumProbUp/float(numValidPredictionsUp)

                        #probability Flat
                        
                        cumProbFlat = 0
                        numValidPredictionsFlat = 0
                        for p in range(0,len(probabilitiesFlat)):
                            probTemp = probabilitiesFlat[p]
                            if probTemp > 0.01:
                                numValidPredictionsFlat += 1
                                cumProbFlat += probTemp
                        if numValidPredictionsFlat > 0:
                            avgProbFlat = cumProbFlat/float(numValidPredictionsFlat)
                    else:
                        avgProbDown = max(probabilitiesDown)
                        avgProbUp = max(probabilitiesUp)
                        avgProbFlat = max(probabilitiesFlat)

                    goodDown = avgProbDown >= kProb
                    goodUp = avgProbUp >= kProb
                    goodFlat = avgProbFlat >= kProb
                    
                    if abs(Pos[assetID]) < epsilon: #if we don't have a position
                        if goodUp:
                            AvgPrice[assetID] = close[assetID]
                            print(  "["+Var[0][0]+"]: ", nbar, "Buy " + AssetNames[assetID] + " @",close[assetID] )
                            if reinvest:
                                Pos[assetID] = (entryMoneyAmount+totalPLAsset[assetID])/close[assetID]
                            else:
                                Pos[assetID] = entryMoneyAmount/close[assetID]
                            if usePLPerc:
                                stopLossOffset = close[assetID]*lossPerc
                                takeProfitOffset = close[assetID]*profitPerc
                                sl[assetID] = close[assetID]-stopLossOffset
                                tp[assetID] = close[assetID]+takeProfitOffset
                            else:
                                sl[assetID] = close[assetID] - slOffset
                                tp[assetID] = close[assetID] + tpOffset
                        elif goodDown:
                            AvgPrice[assetID] = close[assetID]
                            print( "["+Var[0][0]+"]: ", nbar, "Short " + AssetNames[assetID] + " @",close[assetID] )
                            if reinvest:
                                Pos[assetID] = -(entryMoneyAmount+totalPLAsset[assetID])/close[assetID]
                            else:
                                Pos[assetID] = -entryMoneyAmount/close[assetID]
                            if usePLPerc:
                                stopLossOffset = close[assetID]*lossPerc
                                takeProfitOffset = close[assetID]*profitPerc
                                sl[assetID] = close[assetID]+stopLossOffset
                                tp[assetID] = close[assetID]-takeProfitOffset
                            else:
                                sl[assetID] = close[assetID] + slOffset
                                tp[assetID] = close[assetID] - tpOffset
                            
                    else:
                        if Pos[assetID] > epsilon:
                            wentToSL = low[assetID] <= sl[assetID]
                            wentToTP = high[assetID] >= tp[assetID]
                            wentBoth = wentToSL and wentToTP
                            
                            if wentBoth:
                                #closing position at breakeven
                                print( "["+Var[0][0]+"]: ", nbar, "Breakeven",AssetNames[assetID])
                                
                                totalPLAsset[assetID] -= breakEvenCost + slippage + commision
                                Pos[assetID] = 0
                                numLongsBreakeven[assetID] += 1
                            
                            elif wentToSL:
                                #closing position with loss
                                plTemp = (sl[assetID]-AvgPrice[assetID])*Pos[assetID] - slippage - commision
                                totalPLAsset[assetID] += plTemp
                                print( "["+Var[0][0]+"]: ", nbar, "Stop Loss Sell " + AssetNames[assetID] + " @", sl, '$'+str(round(plTemp,2)) )
                                Pos[assetID] = 0
                                numLongsSL[assetID] += 1
                            elif wentToTP:
                                #closing position with profit
                                plTemp = (tp[assetID]-AvgPrice[assetID])*Pos[assetID]  - slippage - commision
                                totalPLAsset[assetID] += plTemp
                                print( "["+Var[0][0]+"]: ", nbar, "Take Profit Sell " + AssetNames[assetID] + " @", tp, '$'+str(round(plTemp,2)) )
                                Pos[assetID] = 0
                                numLongsTP[assetID] += 1
                            elif closingEarly:
                                if goodUp:
                                    print( "["+Var[0][0]+"]: ", nbar, "We continue ",AssetNames[assetID])
                                else:
                                    plTemp = (close[assetID] - AvgPrice[assetID])*Pos[assetID] - slippage - commision
                                    totalPLAsset[assetID] += plTemp
                                    totalEarlyClosePl[assetID] += plTemp
                                    Pos[assetID] = 0
                                    numLongsEarlyClose[assetID] += 1
                                    print( "["+Var[0][0]+"]: ", nbar, "Closing Early",AssetNames[assetID], goodUp, plTemp)

                        elif Pos[assetID] < -epsilon:

                            wentToSL = high[assetID] >= sl[assetID]
                            wentToTP = low[assetID] <= tp[assetID]
                            wentBoth = wentToSL and wentToTP
                            
                            if wentBoth:
                                #closing position at breakeven
                                print( "["+Var[0][0]+"]: ", nbar, "Breakeven",AssetNames[assetID])
                                totalPLAsset[assetID] -= breakEvenCost + slippage + commision
                                Pos[assetID] = 0.0
                                numShortsBreakeven[assetID] += 1
                            
                            elif wentToSL:
                                #closing position with loss
                                plTemp = (AvgPrice[assetID]-sl[assetID])*abs(Pos[assetID]) - slippage - commision
                                totalPLAsset[assetID] += plTemp
                                print( "["+Var[0][0]+"]: ", nbar, "Stop Loss Buy " + AssetNames[assetID] + " @", sl, '$'+str(round(plTemp,2)) )
                                Pos[assetID] = 0.0
                                numShortsSL[assetID] += 1
                            elif wentToTP:
                                #closing position with profit
                                plTemp = (AvgPrice[assetID]-tp[assetID])*abs(Pos[assetID]) - slippage - commision
                                totalPLAsset[assetID] += plTemp
                                print( "["+Var[0][0]+"]: ", nbar, "Take Profit Buy " + AssetNames[assetID] + " @", tp, '$'+str(round(plTemp,2)) )
                                Pos[assetID] = 0.0
                                numShortsTP[assetID] += 1
                            elif closingEarly:
                                if goodDown:
                                    print( "["+Var[0][0]+"]: ", nbar, "We continue ", AssetNames[assetID])
                                else:
                                    plTemp = (AvgPrice[assetID] - close[assetID])*abs(Pos[assetID]) - slippage - commision
                                    totalPLAsset[assetID] += plTemp
                                    totalEarlyClosePl[assetID] += plTemp
                                    Pos[assetID] = 0
                                    numShortsEarlyClose[assetID] += 1
                                    print( "["+Var[0][0]+"]: ", nbar, "Closing Early",AssetNames[assetID], goodDown, plTemp)
        else:
            #second check 
            currentTS += 60000
            if currentTS not in BTC and currentTS not in ETH and currentTS not in LTC:
                cont = False
    
    for assetID in range(firstAsset,lastAsset+1):
        nTotalLongEntries = numLongsTP[assetID]+numLongsSL[assetID]+numLongsBreakeven[assetID]+numLongsEarlyClose[assetID]
        nTotalShortEntries = numShortsTP[assetID]+numShortsSL[assetID]+numShortsBreakeven[assetID]+numShortsEarlyClose[assetID]
        nTotalEntries = nTotalLongEntries+nTotalShortEntries
        plPerEntry = 0.0
        if nTotalEntries:
            plPerEntry = totalPLAsset[assetID]/(float(nTotalEntries))
        print( "Total " + AssetNames[assetID] + " PnL $" + str(round(totalPLAsset[assetID])),"in",nTotalEntries,
         "entries (Profit/Loss PerEntry $", str(round(plPerEntry,3))+ ") PL earlyCloses $" + str(round(totalEarlyClosePl[assetID])) + "[Longs", nTotalLongEntries, 'numLongsTP', numLongsTP[assetID], "numLongsSL", numLongsSL[assetID],
         "numLongsBreakeven", numLongsBreakeven[assetID], "numLongsEarlyClose", numLongsEarlyClose[assetID],"] [Shorts:", nTotalShortEntries, "numShortsTP", numShortsTP[assetID], "numShortsSL", numShortsSL[assetID],  "numShortsBreakeven", numShortsBreakeven[assetID], "numShortsEarlyClose", numShortsEarlyClose[assetID],"]" )

main()