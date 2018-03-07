import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

/**
  * Created by wuyunlong on 17-11-21.
  * 关联规则推荐
  */
object Main extends Serializable{

  def main(args: Array[String]): Unit = {



    val inputD = args(0) //hdfs://127.0.0.1/input/da.dat 输入数据目录数据
    val inputU = args(1) //hdfs://127.0.0.1/input/ua.dat 输入用户数据
    val outputfreqitem = args(2) //hdfs://127.0.0.1/output/freqitemsets 输出频繁项集目录
    val outputrecommend = args(3) //hdfs://127.0.0.1/output/recommenditems 输出推荐商品
    val temdir = args(4) //hdfs://127.0.0.1/temp/Dikaerji 临时结果目录 笛卡尔积


    val conf = new SparkConf().setAppName("MyFPGrowth")
    val sc = new SparkContext(conf)
//    val data = sc.textFile("/home/wuyunlong/data/fpgrowth/da.dat", 3)
    val data = sc.textFile(inputD, 3)
    val transactions: RDD[Array[String]] = data.map(s => s.trim().split(' ')).cache()
    val minSupport=0.2  //最小支持度
    val numPartitions=6  //数据分区
    val minConfidence = 0.8  //最小置行度
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)
    val model = fpg.run(transactions)
    //1、得到频繁项集合
//    model.freqItemsets.saveAsTextFile("/home/wuyunlong/data/FreqItemsets_output")
    model.freqItemsets.saveAsTextFile(outputfreqitem)
    //2、保存生成的关联规则
//    model.generateAssociationRules(minConfidence).saveAsTextFile("/home/wuyunlong/data/AssociationRules_output")
//    model.generateAssociationRules(minConfidence).saveAsTextFile(temdir)
//    val AssociationRuleS = sc.textFile("/home/wuyunlong/data/AssociationRules_output/part-*").cache()
    val AssociationRuleS = model.generateAssociationRules(minConfidence).cache()
    //3、加载用户概貌数据进行关联规则匹配
//    val userdata = sc.textFile("/home/wuyunlong/data/fpgrowth/ua.dat").cache()
    val userdata = sc.textFile(inputU).cache()
//    val allline = userdata.cartesian(AssociationRuleS).saveAsTextFile("/home/wuyunlong/data/Dikaerji")
    val allline = userdata.cartesian(AssociationRuleS).saveAsTextFile(temdir)
    //笛卡尔积后的所有集合
    val dataset = sc.textFile(temdir+"/part-*").cache()
    //处理每一条数据
    val hello = dataset.map { x =>
      //(2257 50385 11599 2956,{878,49,122} => {8}: 0.9751155755950003)
      val recommvalue = x.split(":")(0).toString.split("=>")(1).replace("{", "").replace("}", "") //{8}
      val userbasket = x.split("=>")(0).toString.split(",", 2)(0).replace("(", "").split(" ") //2257 50385 11599 2956
      val transactionitem = x.split(":")(0).toString.split("=>")(0).toString.split(",", 2)(1).replace("{", "").replace("}", "").split(" ")
      //878,49,122
      val confidence = x.split(":")(1).replace(")", "") //0.9751155755950003

      var returnfalg = false
      var length = transactionitem.length.toInt
      //判断是否有推荐的值，并且推荐值还不能在用户购物篮里
      for (ele <- transactionitem) {
        //如果购物篮里面有这个商品，并且要推荐的商品不在购物篮，标志位为true
        if (!userbasket.contains(recommvalue.trim) && userbasket.contains(ele)) {
          length -= 1
        }
      }
      if (length <= 0) {
        returnfalg = true
      }

      (userbasket.mkString(" "), recommvalue, confidence, returnfalg)
    }

    //4、下面rdd1-rdd4就是分组 排序 TOP-1
    val rdd1 = hello.distinct().filter(_._4.equals(true)).map(x => (x._1, x._2, x._3))
    val rdd2 = rdd1.map(x => (x._1, (x._2, x._3))).groupByKey()

    //设置取前TOP-1推荐
    val N_value = 1

    val rdd3 = rdd2.map(x => {
      val i2 = x._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > N_value) i2_2.remove(0, (i2_2.length - N_value))
      (x._1, i2_2.toIterable)
    })

    val rdd4 = rdd3.flatMap(x => {
      val y = x._2
      for (w <- y) yield (x._1, w._1, w._2)
    })

    //在这里处理下，没有产生推荐项的情况
    val nordd = hello.distinct().filter(_._4.equals(false)).map(x => (x._1)).distinct()
    val yesrdd = rdd4.map(x=>(x._1))
    val nonrdd = nordd.subtract(yesrdd).map(x=>(x.toString,"0","0"))
    
    //把nonrdd和rdd4合到一起
    val outputrdd = nonrdd.union(rdd4)
    //根据U.dat顺序保存输出
    val indexuser = userdata.zipWithIndex().map(x=>(x._1,x._2.toString))
    val indexrdd1 = outputrdd.map(x=>(x._1,x._2))
    val result = indexuser.leftOuterJoin(indexrdd1).map(x=>(x._1,x._2._1,x._2._2)).sortBy(_._2).map(x=>(x._1,x._3))

    //输出结果，包含（用户购物篮商品id，推荐项），如果没有推荐项，对应值为0
    result.repartition(1).saveAsTextFile(outputrecommend)
  }

}
