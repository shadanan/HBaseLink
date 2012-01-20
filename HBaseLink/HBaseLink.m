BeginPackage["HBaseLink`", {"JLink`"}]

HBaseLink::usage = "HBaseLink[] is a reference to an open HBase connection"
OpenHBaseLink::usage = "OpenHBaseLink[config rules]"

HBaseListTables::usage = "HBaseListTables[link]"
HBaseListColumns::usage = "HBaseListColumns[link, \"table\"]"
HBaseDescribeTable::usage = "HBaseDescribeTable[link, \"table\"]"

HBaseSetSchema::usage = "HBaseSetSchema[link, \"table\", key -> {\"Decoder\", ...} ...] \
sets the decoding scheme for an HBase table."

HBaseGet::usage = "HBaseGet[link, \"table\", \"key\"]"

HBaseCount::usage = "HBaseCount[link, \"table\"]"

HBaseScan::usage = "HBaseScan[link, \"table\"]"

OpenHBaseLink::connfail = "Unable to establish connection with HBase."
HBaseLink::ischema = "Schema key must be of the form \"key\", {<family>, <qualifier>} or {<family>, <qualifier>, <start>, <stop>}"
HBaseLink::ifield = "Field \"`1`\" must be one of {\"Key\", \"Family\", \"Qualifier\", \"Timestamp\", \"Value\"}"

Begin["`Private`"]
(* Implementation of the package *)

OpenHBaseLink[config : Rule[_String, _String]...] := Module[{hBaseLink},
    InstallJava[];
    LoadJavaClass["java.lang.System", StaticsVisible -> True];
        
    (* Check Java version *)
    With[{javaVersion = System`getProperty["java.version"]},
      If[ToExpression@StringTake[javaVersion, 3] < 1.6,
        die["HBaseLink` requires Java 6 or higher."];
      ]];
        
    (* Create HBaseLink java client *)
    hBaseLink = JavaNew["com.wolfram.hbaselink.HBaseLink", List @@@ {config}];
    If[hBaseLink === $Failed, Message[OpenHBaseLink::connfail]];
    HBaseLink[hBaseLink]]

getConf[h_HBaseLink] := h[[1]]@getConf[]
getCache[h_HBaseLink] := h[[1]]@getCache[]

makeJavaObjectArray[list_List] :=
  Module[{array, i},
    LoadJavaClass["org.apache.hadoop.hbase.util.Bytes", StaticsVisible -> True];
    LoadJavaClass["java.lang.reflect.Array", StaticsVisible -> True];
    array = ReturnAsJavaObject[Array`newInstance[
      JavaNew["java.lang.Object"]@getClass[], Length[list]]];
    For[i = 0, i < Length[list], i += 1,
      Array`set[array, i, MakeJavaObject[list[[i + 1]]]]];
    array]

toBytesBinary[x_String] := 
  JavaBlock[
    LoadJavaClass["org.apache.hadoop.hbase.util.Bytes", StaticsVisible -> True];
    ReturnAsJavaObject[Bytes`toBytesBinary[x]]]

toBytesBinary[x_] /; 
    MatchQ[x@getClass[]@getSimpleName[], "byte[]"] := x

toStringBinary[x_String] := x
    
toStringBinary[x_] /; 
    MatchQ[x@getClass[]@getSimpleName[], "byte[]"] := 
  JavaBlock[
    LoadJavaClass["org.apache.hadoop.hbase.util.Bytes", StaticsVisible -> True];
    Bytes`toStringBinary[x]]

(* Key *)

encodeKey[table_, key_String] := 
  encodeKey[table, toBytesBinary[key]]

encodeKey[table_, key_] /; 
    MatchQ[key@getClass[]@getSimpleName[], "byte[]"] := 
  key

encodeKey[table_, key_] :=
  encodeKey[table, {key}]

encodeKey[table_, key_List] :=
  With[{
      array = makeJavaObjectArray[key]},
    ReturnAsJavaObject[
      table@getKeyTranscoder[]@encode[array]]]

decodeKey[table_, key_String] :=
  decodeKey[table, toBytesBinary[key]]

decodeKey[table_, key_] /; 
    MatchQ[key@getClass[]@getSimpleName[], "byte[]"] :=
  table@getKeyTranscoder[]@decode[key]

(* Family *)

encodeFamily[table_, family_String] := 
  encodeFamily[table, toBytesBinary[family]]

encodeFamily[table_, family_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] := 
  family

encodeFamily[table_, family_] :=
  encodeFamily[table, {family}]

encodeFamily[table_, family_List] :=
  With[{
      array = makeJavaObjectArray[family]},
    ReturnAsJavaObject[
      table@getFamilyTranscoder[]@encode[array]]]

decodeFamily[table_, family_String] :=
  decodeFamily[table, toBytesBinary[family]]
   
decodeFamily[table_, family_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  table@getFamilyTranscoder[]@decode[family]

(* Qualifier *)

encodeQualifier[table_, family_, qualifier_] :=
  encodeQualifier[table, encodeFamily[table, family], qualifier]

encodeQualifier[table_, family_, qualifier_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] := 
  encodeQualifier[table, family, toBytesBinary[qualifier]]

encodeQualifier[table_, family_, qualifier_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] :=
  qualifier

encodeQualifier[table_, family_, qualifier_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  encodeQualifier[table, family, {qualifier}]

encodeQualifier[table_, family_, qualifier_List] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  With[{
      array = makeJavaObjectArray[qualifier]},
    ReturnAsJavaObject[
      table@getQualifierTranscoder[family]@encode[array]]]

decodeQualifier[table_, family_, qualifier_] :=
  decodeQualifier[table, encodeFamily[table, family], qualifier]

decodeQualifier[table_, family_, qualifier_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  decodeQualifier[table, family, toBytesBinary[qualifier]]

decodeQualifier[table_, family_, qualifier_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] :=
  table@getQualifierTranscoder[family]@decode[qualifier]

(* Value w/o Timestamp *)

encodeValue[table_, family_, qualifier_, value_] := 
  encodeValue[table, encodeFamily[table, family], qualifier, value]

encodeValue[table_, family_, qualifier_, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, encodeQualifier[table, family, qualifier], value]

encodeValue[table_, family_, qualifier_, value_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, qualifier, toBytesBinary[value]]

encodeValue[table_, family_, qualifier_, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[value@getClass[]@getSimpleName[], "byte[]"] := 
  value

encodeValue[table_, family_, qualifier_, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, qualifier, {value}]

encodeValue[table_, family_, qualifier_, value_List] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  With[{
      array = makeJavaObjectArray[value]},
    ReturnAsJavaObject[
      table@getTranscoder[family, qualifier]@encode[array]]]

decodeValue[table_, family_, qualifier_, value_] :=
  decodeValue[table, encodeFamily[table, family], qualifier, value]

decodeValue[table_, family_, qualifier_, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  decodeValue[table, family, encodeQualifier[table, family, qualifier], value]

decodeValue[table_, family_, qualifier_, value_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] :=
  decodeValue[table, family, qualifier, toBytesBinary[value]]

decodeValue[table_, family_, qualifier_, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[value@getClass[]@getSimpleName[], "byte[]"] :=
  table@getTranscoder[family, qualifier]@decode[value]

(* Value w/ Timestamp *)

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_] := 
  encodeValue[table, encodeFamily[table, family], qualifier, timestamp, value]

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, encodeQualifier[table, family, qualifier], timestamp, value]

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, qualifier, timestamp, toBytesBinary[value]]

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[value@getClass[]@getSimpleName[], "byte[]"] := 
  value

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  encodeValue[table, family, qualifier, timestamp, {value}]

encodeValue[table_, family_, qualifier_, timestamp_Integer, value_List] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] := 
  With[{
      array = makeJavaObjectArray[value]},
    ReturnAsJavaObject[
      table@getTranscoder[family, qualifier, timestamp]@encode[array]]]

decodeValue[table_, family_, qualifier_, timestamp_Integer, value_] :=
  decodeValue[table, encodeFamily[table, family], qualifier, timestamp, value]

decodeValue[table_, family_, qualifier_, timestamp_Integer, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] :=
  decodeValue[table, family, encodeQualifier[table, family, qualifier], timestamp, value]

decodeValue[table_, family_, qualifier_, timestamp_Integer, value_String] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] :=
  decodeValue[table, family, qualifier, timestamp, toBytesBinary[value]]

decodeValue[table_, family_, qualifier_, timestamp_Integer, value_] /; 
    MatchQ[family@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[qualifier@getClass[]@getSimpleName[], "byte[]"] &&
    MatchQ[value@getClass[]@getSimpleName[], "byte[]"] :=
  table@getTranscoder[family, qualifier, timestamp]@decode[value]

getHBaseAdmin[h_HBaseLink] := JavaBlock[
    JavaNew["org.apache.hadoop.hbase.client.HBaseAdmin", getConf[h]]]

HBaseListTables[h_HBaseLink] := Module[{admin, tables},
    admin = getHBaseAdmin[h];
    tables = admin@listTables[];
    #@getNameAsString[] & /@ tables]

HBaseDescribeTable[h_HBaseLink, tablestr_String] := Module[{admin, table},
    admin = getHBaseAdmin[h];
    table = admin@getTableDescriptor[toBytesBinary[tablestr]];
    {
      "name" -> table@getNameAsString[],
      "families" -> ({
        "name" -> #@getNameAsString[], 
        "bloomFilter" -> #@getBloomFilterType[]@toString[],
        "replicationScope" -> #@getScope[],
        "compression" -> #@getCompression[]@toString[],
        "versions" -> #@getMaxVersions[],
        "timeToLive" -> #@getTimeToLive[],
        "blocksize" -> #@getBlocksize[],
        "inMemory" -> #@isInMemory[],
        "blockCache" -> #@isBlockCacheEnabled[]} & /@ table@getColumnFamilies[])
    }]

HBaseListColumns[h_HBaseLink, tablestr_String] := Module[{admin, tabledesc, columns},
    admin = getHBaseAdmin[h];
    tabledesc = admin@getTableDescriptor[toBytesBinary[tablestr]];
    columns = tabledesc@getColumnFamilies[];
    #@getNameAsString[] & /@ columns]

cacheContains[h_HBaseLink, tablestr_String] :=
    getCache[h]@containsKey[JavaNew["java.lang.String", tablestr]]

cachePut[h_HBaseLink, key_String, value_] :=
    getCache[h]@put[JavaNew["java.lang.String", key], value]

cacheGet[h_HBaseLink, key_String] :=
    getCache[h]@get[JavaNew["java.lang.String", key]]

cacheRemove[h_HBaseLink, key_String] :=
    getCache[h]@remove[JavaNew["java.lang.String", key]]

getHBaseHTable[h_HBaseLink, tablestr_String] := 
    If[cacheContains[h, tablestr],
      cacheGet[h, tablestr],
      With[{table = JavaNew["com.wolfram.hbaselink.HTable", getConf[h], toBytesBinary[tablestr]]},
        cachePut[h, tablestr, table];
        table]]

clearHBaseHTable[h_HBaseLink, tablestr_String] :=
    cacheRemove[h, tablestr]

getTranscoder[params__] :=
  JavaNew["com.wolfram.hbaselink." <> First @ params, Sequence @@ (Rest @ params)];

Options[HBaseSetSchema] = {
  "Key" -> None,
  "Family" -> None,
  "Qualifiers" -> None,
  "Columns" -> None
}

HBaseSetSchema[h_HBaseLink, tablestr_String, opts : OptionsPattern[HBaseSetSchema]] := 
    Module[
      {table, index, key, transcoder},
      
      clearHBaseHTable[h, tablestr];
      table = getHBaseHTable[h, tablestr];
      
      If[OptionValue["Key"] =!= None,
        transcoder = getTranscoder[OptionValue["Key"]];
        If[transcoder === $Failed, Return[$Failed]];
        table@setKeyTranscoder[transcoder]];
      
      If[OptionValue["Family"] =!= None,
        transcoder = getTranscoder[OptionValue["Family"]];
        If[transcoder === $Failed, Return[$Failed]];
        table@setFamilyTranscoder[transcoder]];
      
      If[OptionValue["Qualifiers"] =!= None,
        For[index = 1, index <= Length[OptionValue["Qualifiers"]], index += 1,
          transcoder = getTranscoder[OptionValue["Qualifiers"][[index, 2]]];
          If[transcoder === $Failed, Return[$Failed]];
	      table@setQualifierTranscoder[
	        toBytesBinary[OptionValue["Qualifiers"][[index, 1]]], transcoder]]];
      
      If[OptionValue["Columns"] =!= None,
        For[index = 1, index <= Length[OptionValue["Columns"]], index += 1,
          transcoder = getTranscoder[OptionValue["Columns"][[index, 2]]];
          If[transcoder === $Failed, Return[$Failed]];
          
          key = OptionValue["Columns"][[index, 1]];
        
          Which[
            MatchQ[key, List[_String]],
            table@setTranscoder[toBytesBinary[key[[1]]], Null, transcoder],
        
            MatchQ[key, List[_String, _String]],
            table@setTranscoder[toBytesBinary[key[[1]]], toBytesBinary[key[[2]]], transcoder],
        
            MatchQ[key, List[_String, _Integer, _Integer]],
            table@setTranscoder[toBytesBinary[key[[1]]], Null, key[[2]], key[[3]], transcoder],
        
            MatchQ[key, List[_String, _String, _Integer, _Integer]],
            table@setTranscoder[toBytesBinary[key[[1]]], toBytesBinary[key[[2]]], key[[3]], key[[4]], transcoder],
          
            True,
            Message[HBaseLink::ischema];
            Return[$Failed];
          ]
        ]
      ]
    ]

Options[setIncludeExclude] = {
  "Include" -> Automatic,
  "Exclude" -> Automatic
}

setIncludeExclude[table_, opts : OptionsPattern[setIncludeExclude]] :=
  Module[{validFields, includes},
    validFields = {"Key", "Family", "Qualifier", "Timestamp", "Value"};
    Message[HBaseLink::ifield, #] & /@ Complement[Complement[Flatten[Union[
      {OptionValue["Include"]}, {OptionValue["Exclude"]}]], {Automatic}], validFields];
    
    includes = Which[
      MatchQ[OptionValue["Include"], List[_String ...]],
      Intersection[validFields, OptionValue["Include"]],
      
      MatchQ[OptionValue["Include"], _String],
      Intersection[validFields, {OptionValue["Include"]}],
        
      True, validFields];
    
    includes = Which[
      MatchQ[OptionValue["Exclude"], List[_String ...]],
      Complement[includes, OptionValue["Exclude"]],
      
      MatchQ[OptionValue["Exclude"], _String],
      Complement[includes, {OptionValue["Exclude"]}],
        
      True, includes];
    
    table@setExcludeAll[];
    With[{method = ToExpression["setInclude" <> #]},
      table@method[True]] & /@ includes;
  ]

Options[HBaseGet] = {
  "Columns" -> None,
  "Families" -> None,
  "Column" -> None,
  "Family" -> None,
  "Versions" -> 1,
  "TimeStamp" -> None,
  "TimeRange" -> None,
  "Exclude" -> "Key"
}

HBaseGet[h_HBaseLink, tablestr_String, key_, opts : OptionsPattern[{HBaseGet, setIncludeExclude}]] := 
  Module[{table, get},
    table = getHBaseHTable[h, tablestr];
    
    setIncludeExclude[table, FilterRules[{opts}, Options[setIncludeExclude]]];
    
    get = JavaNew["org.apache.hadoop.hbase.client.Get", 
      encodeKey[table, key]];
    If[OptionValue["Families"] =!= None,
      get@addFamily[
        encodeFamily[table, #]] & /@ OptionValue["Families"]];
    If[OptionValue["Columns"] =!= None,
      get@addColumn[
        encodeFamily[table, #[[1]]], 
        encodeQualifier[table, #[[1]], #[[2]]]] & /@ OptionValue["Columns"]];
    If[OptionValue["Family"] =!= None,
      get@addFamily[
        encodeFamily[table, OptionValue["Family"]]]];
    If[OptionValue["Column"] =!= None,
      get@addColumn[
        encodeFamily[table, OptionValue["Column"][[1]]], 
        encodeQualifier[table, OptionValue["Column"][[1]], OptionValue["Column"][[2]]]]];
    
    get@setMaxVersions[OptionValue["Versions"]];
    If[OptionValue["TimeStamp"] =!= None,
      get@setTimeStamp[OptionValue["TimeStamp"]]];
    If[OptionValue["TimeRange"] =!= None,
      get@setTimeRange[OptionValue["TimeRange"][[1]], OptionValue["TimeRange"][[2]]]];
      
    table@getDecoded[get]]

Options[HBaseCount] = {
  "CachingRows" -> 1000
}

HBaseCount[h_HBaseLink, tablestr_String, opts : OptionsPattern[HBaseCount]] := 
  Module[{table},
    table = getHBaseHTable[h, tablestr];
    Monitor[table@countDecoded[OptionValue["CachingRows"]], Refresh[StringJoin[{
        "Count: ", ToString[table@getCurrentCount[]], "  Row: ", ToString[table@getCurrentRow[]]}], 
      UpdateInterval -> 0.5]]]

Options[HBaseScan] = {
  "StartRow" -> None,
  "StopRow" -> None,
  "Columns" -> None,
  "Families" -> None,
  "Column" -> None,
  "Family" -> None,
  "Versions" -> 1,
  "TimeStamp" -> None,
  "TimeRange" -> None,
  "Filter" -> None,
  "CacheBlocks" -> True,
  "Limit" -> All
}

HBaseScan[h_HBaseLink, tablestr_String, opts : OptionsPattern[{HBaseScan, setIncludeExclude}]] :=
  Module[{table, scan, limit},
    table = getHBaseHTable[h, tablestr];
    
    setIncludeExclude[table, FilterRules[{opts}, Options[setIncludeExclude]]];
    
    scan = JavaNew["org.apache.hadoop.hbase.client.Scan"];
    If[OptionValue["StartRow"] =!= None,
      scan@setStartRow[
        encodeKey[table, OptionValue["StartRow"]]]];
    If[OptionValue["StopRow"] =!= None,
      scan@setStopRow[
        encodeKey[table, OptionValue["StopRow"]]]];
    If[OptionValue["Families"] =!= None,
      scan@addFamily[
        encodeFamily[table, #]] & /@ OptionValue["Families"]];
    If[OptionValue["Columns"] =!= None,
      scan@addColumn[
        encodeFamily[table, #[[1]]], 
        encodeQualifier[table, #[[1]], #[[2]]]] & /@ OptionValue["Columns"]];
    If[OptionValue["Family"] =!= None,
      scan@addFamily[
        encodeFamily[table, OptionValue["Family"]]]];
    If[OptionValue["Column"] =!= None,
      scan@addColumn[
        encodeFamily[table, OptionValue["Column"][[1]]], 
        encodeQualifier[table, OptionValue["Column"][[1]], OptionValue["Column"][[2]]]]];
    If[And @@ (OptionValue[#] === None & /@ {"Columns", "Families", "Column", "Family"}),
      scan@addFamily[
        encodeFamily[table, #]] & /@ HBaseListColumns[h, tablestr]];

    If[OptionValue["Filter"] =!= None,
      scan@setFilter[OptionValue["Filter"]]];
    scan@setCacheBlocks[OptionValue["CacheBlocks"]];
    scan@setMaxVersions[OptionValue["Versions"]];
    If[OptionValue["TimeStamp"] =!= None,
      scan@setTimeStamp[OptionValue["TimeStamp"]]];
    If[OptionValue["TimeRange"] =!= None,
      scan@setTimeRange[OptionValue["TimeRange"][[1]], OptionValue["TimeRange"][[2]]]];
    
    table@setScan[scan];
    limit = If[OptionValue["Limit"] === All, -1, OptionValue["Limit"]];
    Monitor[table@scanDecoded[limit], Refresh[StringJoin[{
      "Count: ", ToString[table@getCurrentCount[]], "  Row: ", ToString[table@getCurrentRow[]]}], 
      UpdateInterval -> 0.5]]]

End[]

EndPackage[]

