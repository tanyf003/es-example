curl -XPUT '192.168.1.106:9200/_river/my_jdbc_river/_meta' -d '{
    "type" : "jdbc",
    "jdbc" : {
        "url" : "jdbc:mysql://192.168.1.106:3306/hbres",
        "user" : "root",
        "password" : "root",
        "sql" : "select sex,class_oper,tel,_MASK_TO_V2,userid,areacode,mysqlid,id,returnvist_statu,username,asktime,title,is_change,is_finished,qq,classid,sid3,sid2,mobil,sid1,id as _id from fl_ask",
        "index" : "fl_ask_2",
        "type" : "ask",
        "type_mapping":{"ask":{"_all":{"indexAnalyzer":"mmseg","searchAnalyzer":"mmseg","term_vector":"with_positions_offsets","store":true,"enabled":true},"properties": {
		"sex": {
		"type": "long"
		},
		"class_oper": {
		"type": "string"
		},
		"tel": {
		"type": "string"
		},
		"_MASK_TO_V2": {
		"format": "dateOptionalTime",
		"type": "date"
		},
		"userid": {
		"type": "string"
		},
		"areacode": {
		"type": "long"
		},
		"mysqlid": {
		"type": "long"
		},
		"id": {
		"type": "long"
		},
		"returnvist_statu": {
		"type": "long"
		},
		"username": {
		"type": "string"
		},
		"asktime": {
		"type": "long"
		},
		"title": {
		"type": "string",
		"include_in_all":true
		},
		"is_change": {
		"type": "long"
		},
		"is_finished": {
		"type": "long"
		},
		"qq": {
		"type": "string"
		},
		"classid": {
		"type": "string"
		},
		"sid3": {
		"type": "long"
		},
		"sid2": {
		"type": "long"
		},
		"mobil": {
		"type": "string"
		},
		"sid1": {
		"type": "long"
		}
		}}}
    }
}'


