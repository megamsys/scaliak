package com.stackmob.scaliak.mapreduce

import com.basho.riak.pbc.mapreduce.JavascriptFunction._

object MapReduceFunctions {
	def mapValuesToJson(keep: Boolean = true) = MapPhase(named("Riak.mapValuesJson"), keep)
	def mapValuesToJson:MapPhase = mapValuesToJson(true)
	
	def filterNotFound(keep: Boolean = true) = ReducePhase(named("Riak.filterNotFound"), keep)
	def filterNotFound:ReducePhase = filterNotFound(true)
	
	def sort(field: String, sortDESC: Boolean = false, keep: Boolean = true) = ReducePhase(anon(""" 
	function(values) {
	 
	 var field = """" + field + """";
	 var reverse = """ + sortDESC.toString + """;
	 return values.sort(function(a, b) {
	   if (reverse) {
	     var _ref = [b, a];
	     a = _ref[0];
	     b = _ref[1];
	   }
	   if (((typeof a === "undefined" || a === null) ? undefined :
	a[field]) < ((typeof b === "undefined" || b === null) ? undefined :
	b[field])) {
	     return -1;
	   } else if (((typeof a === "undefined" || a === null) ? undefined :
	a[field]) === ((typeof b === "undefined" || b === null) ? undefined :
	b[field])) {
	     return 0;
	   } else if (((typeof a === "undefined" || a === null) ? undefined :
	a[field]) > ((typeof b === "undefined" || b === null) ? undefined :
	b[field])) {
	     return 1;
	   }
	 });
	}    
	"""), keep)
}