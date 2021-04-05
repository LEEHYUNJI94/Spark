//RDD load
val transactionFile = sc.textfile("../transactiondata.txt")
val transactionData = transactionFile.map(._split(",")) //데이터파싱
val transactionCustId = transactionData.map(trans => (._2,toInt,trans) //PairRDD 생성 (tuple)

//구매횟수 많은 고객 선정 후 사은품 증정로직 추가 (compTrnasaction)
val (custId, purch ) = transactionCustId.countByKey().toSeq.sortBy(_._2).last
var compTransaction = Array(Array("2018-10-25","11:50 PM","53","4","1","0.00"))
transactionCustId.lookup(53).foreach(tran => println(tran.mkString(","))) //ID 53번 고객의 구매기록
/*
각 고객이 구매한 제품의 전체목록
aggregateByKey- Zero value & 변환함수,병합함수를 인수로 전달 & 인자를 여러목록으로 나눔 = 컬링
*/
val prods = transactionCustId.aggregateByKey(List[String]())( 
            (prods , trans) => prods ::: List(trans(3)), // ::: 리스트를 이어붙임
            (prods1 , prods2) => prods1 ::: prods2) // 키가같은 리스트를 붙임
)

/*
(itemID=25) 구매 시 30%할인 
- MapValues :Key를 변경하지 않고 pairRDD 값변경가능
*/
transactionCustId = transactionCustId.mapValues(trans => {
					if(trans(3).toInt == 25 && trans(4).toDouble > 1)
						trans(5) = (trans(5).toInt * 0.7).toString
					trans
					})
/*
(itemID=81) * 5 구매시 사은품 전송 (itemID=70) 
-flatMapvalues : key값을 0개 혹은 1개이상값으로 매핑 -> RDD에 포함된 요소 개수 변경가능
*/
transactionCustId = transactionCustId.flatMapValues(trans => {
                    if (trans(5).toInt == 81 && trans(4).toDouble >=5 ){
                        val cloned = trans.clone() //구매기록 배열 복제
                        cloned(5) = "0.00" ; cloned(5) = "70" ; cloned(4) = "1" ; 
                        List(trans,cloned)
                    }
                    else //조건 만족 X - 원래 요소반환
                        List(trans)
                    })
/*
가장 많은 금액을 지출한 고객 추출(76) -> 사은품 증정 로직 
TransactionCustId에 저장된 구매기록은 문자열 배열 ->tuple로 바꿔야함
-foldByKey: reduceKey와 동일하지만 zeroValue인자 목록추가필요 foledByKey(0) or foledByKey(1000)
*/
val amounts = transactionCustId.map(t => t(5).toDouble) //data mapping : 튜플에 금액정보만 포함
val totals = amounts.foledByKey(0)((p1,p2)=>(p1+p2)).collect
totals.toSeq.sortBy(_._2).last //Result : (76,10049.0)

compTransaction = compTransaction :+Array("2018-10-25","11:50 PM","76","63","1","0.00")

/*
compTransaction에 있는 배열을 transactionCustId RDD에 추가 후 새로운 파일로 저장
*/
transactionCustId = transactionCustId.union(sc.parallelize(compTransaction)
                                            .map(t => (t(2).toInt,t))
                                            )
transactionCustId.map(t => t._2.mkString(",")).saveAsFile("transCustId_promotion.txt")
