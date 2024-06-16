package es

import (
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"monthzc/common/viper"
	"monthzc/models/mysql"
	"strconv"
)

var (
	Es  *elastic.Client
	err error
)

func InitEs() {
	cfg := viper.LoadViper()
	Es, err = elastic.NewClient(elastic.SetURL(fmt.Sprintf("http://%s:%d", cfg.EsHost, cfg.EsPort)), elastic.SetSniff(cfg.Sniff))
	if err != nil {
		log.Fatal("es连接失败")
		return
	}
	all, _ := mysql.GoodsAll()
	for _, v := range all {
		GoodsAdd(strconv.Itoa(int(v.ID)), v)
	}
	log.Println("es连接成功")
}
------------------------------------------------------------------------------------------------------------
// GoodsAdd es数据同步
func GoodsAdd(id string, goods *mysql.Goods) bool {
	_, err = Es.Index().Index("goods").Id(id).BodyJson(goods).Do(context.Background())
	if err != nil {
		return false
	}
	return true
}

// GoodsSearch 全文、分页、高亮搜索
func GoodsSearch(value string, p int) []map[string]interface{} {
	do, _ := Es.Search().Index("goods").Query(elastic.NewQueryStringQuery(value)).From((p - 1) * 2).Size(2).Highlight(elastic.NewHighlight().Field("GoodsName").PreTags("<span color ='red'>").PostTags("</span>")).Do(context.Background())
	var data []map[string]interface{}
	for _, v := range do.Hits.Hits {
		var d map[string]interface{}
		json.Unmarshal(v.Source, &d)
		d["GoodsName"] = v.Highlight
		data = append(data, d)
	}
	return data
}

// GoodsSort 根据字段排序
func GoodsSort(field string) []map[string]interface{} {
	do, _ := Es.Search().Index("goods").Query(elastic.NewMatchAllQuery()).Sort(field, false).Do(context.Background())
	var data []map[string]interface{}
	for _, v := range do.Hits.Hits {
		var d map[string]interface{}
		json.Unmarshal(v.Source, &d)
		data = append(data, d)
	}
	return data
}
