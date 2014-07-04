package de.unimannheim.dws.algorithms

case class ModelEval (
    val clusterInfo: String,
    val time: Long,
    val triples: List[((String, String, String), String)]) {
    
    def getTime = time
    
    /*
     * TODO:
     * getNoiseRel
     * getNoClusters
     * getElemCluster
     * getElemClustSD
     * getElemClustVar
     * 
     * 
     *  
     */
    
    def getNoiseRel = {
      val noiseElems = triples.filter(t => t._2.equals("noise"))
      
    }
    
}

