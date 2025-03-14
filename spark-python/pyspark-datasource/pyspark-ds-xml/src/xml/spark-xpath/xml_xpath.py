from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("xml_xpath").getOrCreate()

    data = [
        "<ns0:RespNewCreditEvaluation xmlns:ns0=\"http://eda.experian.com/bpc/\"><InboundResponse><DV-Application"
        "><ApplicationNumber>000000008767050</ApplicationNumber><ApplicationCreationDate>2023-03-21"
        "</ApplicationCreationDate><ApplicationCreationTime>07:41:43:420</ApplicationCreationTime"
        "><ApplicationUpdateDate>2023-03-21</ApplicationUpdateDate><ApplicationUpdateTime>07:41:44:620"
        "</ApplicationUpdateTime><ApplicationStatus>Approved</ApplicationStatus><ReferenceNumber>zppCi1214d230"
        "</ReferenceNumber></DV-Application><DV-Results><RST "
        "cxArrayIndex=\"1\"><CreditClass>INN88</CreditClass><LiabilityIndicator>I</LiabilityIndicator"
        "><RelationshipIndicator>N</RelationshipIndicator><PortIndicator>N</PortIndicator><InitialRiskGroup>8"
        "</InitialRiskGroup><CurrentRiskGroup>8</CurrentRiskGroup><Rdm-Nbr-1>764</Rdm-Nbr-1><Rdm-Nbr-2>782</Rdm-Nbr-2"
        "><Rdm-Nbr-3>886</Rdm-Nbr-3><Rdm-Nbr-4>913</Rdm-Nbr-4><Rdm-Nbr-5>0</Rdm-Nbr-5><DecisionCategory>Approve"
        "</DecisionCategory><DecisionText>Approve</DecisionText><SortedReasonCodeTable "
        "cxArrayIndex=\"1\">A001</SortedReasonCodeTable></RST></DV-Results><BureauResult><EXP><FrozenFileInd>N"
        "</FrozenFileInd><ExpCons-XFC01>0</ExpCons-XFC01><ExpCons-XFC06>0</ExpCons-XFC06><ExpCons-XFC07>0</ExpCons"
        "-XFC07><MLAIndicator>N</MLAIndicator><ReportBlockedIndicator>N</ReportBlockedIndicator"
        "><ReportLockedIndicator>N</ReportLockedIndicator><NoHit>N</NoHit><BigFileIndicator>N</BigFileIndicator"
        "><ActiveDutyAlertIndicator>N</ActiveDutyAlertIndicator><RiskModel "
        "cxArrayIndex=\"1\"><ScoreID>TC</ScoreID><Score>533</Score><ReasonCode1>74</ReasonCode1><ReasonCode2>69"
        "</ReasonCode2><ReasonCode3>02</ReasonCode3><ReasonCode4>50</ReasonCode4><Evaluation>P</Evaluation"
        "></RiskModel></EXP><EQU><EquifaxNoScore>N</EquifaxNoScore><FrozenFileInd>N</FrozenFileInd><MLAInd>N</MLAInd"
        "><ReportBlockedInd>N</ReportBlockedInd><ReportLockedInd>N</ReportLockedInd><NoHit>N</NoHit><BigFileIndicator"
        ">N</BigFileIndicator><ActiveDutyAlertIndicator>N</ActiveDutyAlertIndicator><RiskModel "
        "cxArrayIndex=\"1\"><Score>590</Score><ModelID>4</ModelID><ModelNumber>5322</ModelNumber><ScoreNb>1</ScoreNb"
        "><ReasonCode1>206</ReasonCode1><ReasonCode2>293</ReasonCode2><ReasonCode3>352</ReasonCode3><ReasonCode4>205"
        "</ReasonCode4></RiskModel></EQU></BureauResult></InboundResponse></ns0:RespNewCreditEvaluation>",
        "<ns0:RespNewCreditEvaluation xmlns:ns0=\"http://eda.experian.com/bpc/\"><InboundResponse><DV-Application"
        "><ApplicationNumber>000000008767044</ApplicationNumber><ApplicationCreationDate>2023-03-21"
        "</ApplicationCreationDate><ApplicationCreationTime>07:33:44:404</ApplicationCreationTime"
        "><ApplicationUpdateDate>2023-03-21</ApplicationUpdateDate><ApplicationUpdateTime>07:33:45:609"
        "</ApplicationUpdateTime><ApplicationStatus>Approved</ApplicationStatus><ReferenceNumber"
        ">VJVas5931148762931572027042021.IMS</ReferenceNumber></DV-Application><DV-Results><RST "
        "cxArrayIndex=\"1\"><CreditClass>INN00</CreditClass><LiabilityIndicator>I</LiabilityIndicator"
        "><RelationshipIndicator>N</RelationshipIndicator><PortIndicator>N</PortIndicator><InitialRiskGroup>0"
        "</InitialRiskGroup><CurrentRiskGroup>0</CurrentRiskGroup><Rdm-Nbr-1>674</Rdm-Nbr-1><Rdm-Nbr-2>166</Rdm-Nbr-2"
        "><Rdm-Nbr-3>687</Rdm-Nbr-3><Rdm-Nbr-4>100</Rdm-Nbr-4><Rdm-Nbr-5>0</Rdm-Nbr-5><DecisionCategory>Approve"
        "</DecisionCategory><DecisionText>Approve</DecisionText><SortedReasonCodeTable "
        "cxArrayIndex=\"1\">A001</SortedReasonCodeTable></RST></DV-Results><BureauResult><EQU><EquifaxNoScore>N"
        "</EquifaxNoScore><FrozenFileInd>N</FrozenFileInd><MLAInd>N</MLAInd><ReportBlockedInd>N</ReportBlockedInd"
        "><ReportLockedInd>N</ReportLockedInd><NoHit>N</NoHit><BigFileIndicator>N</BigFileIndicator"
        "><ActiveDutyAlertIndicator>N</ActiveDutyAlertIndicator><RiskModel "
        "cxArrayIndex=\"1\"><Score>990</Score><ModelID>4</ModelID><ModelNumber>5322</ModelNumber><ScoreNb>1</ScoreNb"
        "><ReasonCode1>308</ReasonCode1><ReasonCode2>123</ReasonCode2><ReasonCode3>147</ReasonCode3><ReasonCode4>68"
        "</ReasonCode4></RiskModel></EQU></BureauResult></InboundResponse></ns0:RespNewCreditEvaluation>",
        "<ns0:RespNewCreditEvaluation xmlns:ns0=\"http://eda.experian.com/bpc/\"><InboundResponse><DV-Application"
        "><ApplicationNumber>000000008784476</ApplicationNumber><ApplicationCreationDate>2023-03-21"
        "</ApplicationCreationDate><ApplicationCreationTime>20:34:58:173</ApplicationCreationTime"
        "><ApplicationUpdateDate>2023-03-21</ApplicationUpdateDate><ApplicationUpdateTime>20:35:01:803"
        "</ApplicationUpdateTime><ApplicationStatus>Approved</ApplicationStatus><ReferenceNumber"
        ">o5ZD8179002021401052012RLcsg01002</ReferenceNumber></DV-Application><DV-Results><RST "
        "cxArrayIndex=\"1\"><CreditClass>BNY66</CreditClass><LiabilityIndicator>B</LiabilityIndicator"
        "><RelationshipIndicator>N</RelationshipIndicator><PortIndicator>Y</PortIndicator><InitialRiskGroup>6"
        "</InitialRiskGroup><CurrentRiskGroup>6</CurrentRiskGroup><Rdm-Nbr-1>594</Rdm-Nbr-1><Rdm-Nbr-2>300</Rdm-Nbr-2"
        "><Rdm-Nbr-3>266</Rdm-Nbr-3><Rdm-Nbr-4>411</Rdm-Nbr-4><Rdm-Nbr-5>0</Rdm-Nbr-5><DecisionCategory>Approve"
        "</DecisionCategory><DecisionText>Approve</DecisionText><SortedReasonCodeTable "
        "cxArrayIndex=\"1\">A001</SortedReasonCodeTable></RST></DV-Results><BureauResult><EXP><BISScore>3300"
        "</BISScore><BISScoreID>000224</BISScoreID><BISScoreReasonCode1>001 "
        "</BISScoreReasonCode1><BISScoreReasonCode2>055 </BISScoreReasonCode2><BISScoreReasonCode3>002 "
        "</BISScoreReasonCode3><BISScoreReasonCode4>044 "
        "</BISScoreReasonCode4><FSRScore>800</FSRScore><FSRScoreID>000223</FSRScoreID><FSRScoreReasonCode1>009 "
        "</FSRScoreReasonCode1><FSRScoreReasonCode2>008 </FSRScoreReasonCode2><FSRScoreReasonCode3>004 "
        "</FSRScoreReasonCode3><FSRScoreReasonCode4>002 "
        "</FSRScoreReasonCode4><BISAttributes><ATC032>0</ATC032><ACC001>0</ACC001><ATB001>0</ATB001><ATB002>0</ATB002"
        "><ATB003>0</ATB003><ATB004>0</ATB004><ATB005>0</ATB005><ATB006>0</ATB006><BRC010>0</BRC010><BRO017>0</BRO017"
        "><BRB005>0</BRB005><BRB003>0</BRB003><BKC006>0</BKC006><CLB001>0</CLB001><TTB023>0</TTB023><TTB024>0</TTB024"
        "><TTB025>0</TTB025><TTB026>0</TTB026><TTB027>0</TTB027><TTO077>351</TTO077><ACC007>0</ACC007><ACC008>0"
        "</ACC008><UCC002>3</UCC002><UCC003>0</UCC003></BISAttributes></EXP></BureauResult></InboundResponse></ns0"
        ":RespNewCreditEvaluation>",
        "<ns0:RespNewCreditEvaluation xmlns:ns0=\"http://eda.experian.com/bpc/\"><InboundResponse><DV-Application"
        "><ApplicationNumber>000000008784481</ApplicationNumber><ApplicationCreationDate>2023-03-21"
        "</ApplicationCreationDate><ApplicationCreationTime>20:35:30:823</ApplicationCreationTime"
        "><ApplicationUpdateDate>2023-03-21</ApplicationUpdateDate><ApplicationUpdateTime>20:35:31:468"
        "</ApplicationUpdateTime><ApplicationStatus>Approved</ApplicationStatus><ReferenceNumber"
        ">WhYvZNCOLAVWTEG1679425894AT</ReferenceNumber></DV-Application><DV-Results><RST "
        "cxArrayIndex=\"1\"><CreditClass>INN22</CreditClass><LiabilityIndicator>I</LiabilityIndicator"
        "><RelationshipIndicator>N</RelationshipIndicator><PortIndicator>N</PortIndicator><InitialRiskGroup>2"
        "</InitialRiskGroup><CurrentRiskGroup>2</CurrentRiskGroup><Rdm-Nbr-1>233</Rdm-Nbr-1><Rdm-Nbr-2>421</Rdm-Nbr-2"
        "><Rdm-Nbr-3>615</Rdm-Nbr-3><Rdm-Nbr-4>985</Rdm-Nbr-4><Rdm-Nbr-5>0</Rdm-Nbr-5><DecisionCategory>Approve"
        "</DecisionCategory><DecisionText>Approve</DecisionText><SortedReasonCodeTable "
        "cxArrayIndex=\"1\">A001</SortedReasonCodeTable></RST></DV-Results><BureauResult><EQU><EquifaxNoScore>N"
        "</EquifaxNoScore><FrozenFileInd>N</FrozenFileInd><MLAInd>N</MLAInd><ReportBlockedInd>N</ReportBlockedInd"
        "><ReportLockedInd>N</ReportLockedInd><NoHit>N</NoHit><BigFileIndicator>N</BigFileIndicator"
        "><ActiveDutyAlertIndicator>N</ActiveDutyAlertIndicator><RiskModel "
        "cxArrayIndex=\"1\"><Score>917</Score><ModelID>4</ModelID><ModelNumber>5322</ModelNumber><ScoreNb>1</ScoreNb"
        "><ReasonCode1>76</ReasonCode1><ReasonCode2>293</ReasonCode2><ReasonCode3>206</ReasonCode3><ReasonCode4>205"
        "</ReasonCode4></RiskModel></EQU></BureauResult></InboundResponse></ns0:RespNewCreditEvaluation>"
    ]

    # INQUIRY_DATE
    xml_df = spark.createDataFrame(data, StringType()) \
        .withColumnRenamed("value", "data")

    xml_df.createOrReplaceTempView("xml_data")

    spark.sql("select * from xml_data").show(truncate=False)

    spark.sql("""with riskModeCalc as 
                (   select 
                        data,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/DV-Application/ApplicationCreationDate\') as INQUIRY_DATE,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/DV-Application/ApplicationStatus\') as REPORT_TYPE,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/DV-Results/RST[@cxArrayIndex=1]/LiabilityIndicator\') as LIABILITY_TYPES,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ModelID\') as eqfRiskModelId,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/Score\') as eqfScore,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ModelNumber\') as eqfModelNumber,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ReasonCode1\') as eqfReasonCode1,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ReasonCode2\') as eqfReasonCode2,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ReasonCode3\') as eqfReasonCode3,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1]/ReasonCode4\') as eqfReasonCode4,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EXP/RiskModel[@cxArrayIndex=1]/ScoreID\') as evsRiskModel,
                        xpath_boolean(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EQU/RiskModel[@cxArrayIndex=1][ ModelID >= 2 and ModelID <= 4 ]\') as isEqfRiskModel,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EXP/RiskModel[@cxArrayIndex=1]/ScoreID\') == \'EV\'  as isEvsRiskModel,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EXP/RiskModel[@cxArrayIndex=1]/Score\')  as Score,
                        xpath_string(data, \'RespNewCreditEvaluation/InboundResponse/BureauResult/EXP/RiskModel[@cxArrayIndex=1]/ScoreID\') == \'TC\'  as isTecRiskModel
                    from 
                        xml_data
                )
                select *,
                 case 
                    when isEqfRiskModel and isEvsRiskModel then eqfScore
                    when isTecRiskModel then Score
                    else Score
                end,
                 isTecRiskModel and (instr(Score,"9") == 1  or (cast(Score as int) >=0 and cast(Score as int) <1000)) as isTecRiskModelFull from riskModeCalc
        """)\
        .show(truncate=False)
