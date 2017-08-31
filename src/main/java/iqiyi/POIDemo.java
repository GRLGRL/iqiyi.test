package iqiyi;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.util.CellRangeAddress;

import java.util.List;
import java.util.ArrayList;

/**
 * Created by gaoronglei_sx on 2017/8/30.
 */
public class POIDemo
{
    private static List<String> dateMap = new ArrayList<String>();
    private static String HIVE_RS_headline_Every = "/home/qytt/work_mapreduce/gaoronglei/HIVE_RS_headline_Every";
    private static String HIVE_RS_ttbrain_Every = "/home/qytt/work_mapreduce/gaoronglei/HIVE_RS_ttbrain_Every";
    private static String HIVE_RS_headline_Sum = "/home/qytt/work_mapreduce/gaoronglei/HIVE_RS_headline_Sum";
    private static String HIVE_RS_ttbrain_Sum = "/home/qytt/work_mapreduce/gaoronglei/HIVE_RS_ttbrain_Sum";
    public static void main(String[] args)
    {
        setTime();
        HSSFWorkbook excel = new HSSFWorkbook();
        HSSFSheet sheet1 = excel.createSheet("总信息报表");
        HSSFSheet sheet2 = excel.createSheet("内容池_推荐池_报表");
        for(int i=0;i<11;i++)
        {
            HSSFRow row = sheet1.createRow(i);
            switch (i)
            {
                case 0:{
                     row.createCell(0).setCellValue("");
                     row.createCell(1).setCellValue("类型");
                     row.createCell(2).setCellValue(dateMap.get(0));
                     row.createCell(3).setCellValue(dateMap.get(1));
                     row.createCell(4).setCellValue(dateMap.get(2));
                     row.createCell(5).setCellValue(dateMap.get(3));
                     row.createCell(6).setCellValue(dateMap.get(4));
                     row.createCell(7).setCellValue(dateMap.get(5));
                     row.createCell(8).setCellValue(dateMap.get(6));
                     row.createCell(9).setCellValue("最近7天新增");
                    break;
                }
                case 1:{
                    row.createCell(0).setCellValue("");
                    row.createCell(1).setCellValue("ppc");
                    row.createCell(2).setCellValue("");
                    row.createCell(3).setCellValue("");
                    row.createCell(4).setCellValue("");
                    row.createCell(5).setCellValue("");
                    row.createCell(6).setCellValue("");
                    row.createCell(7).setCellValue("");
                    row.createCell(8).setCellValue("");
                    row.createCell(9).setCellValue("");
                    break;
                }
                case 2:{
                    row.createCell(0).setCellValue("");
                    row.createCell(1).setCellValue("pgc");
                    row.createCell(2).setCellValue("");
                    row.createCell(3).setCellValue("");
                    row.createCell(4).setCellValue("");
                    row.createCell(5).setCellValue("");
                    row.createCell(6).setCellValue("");
                    row.createCell(7).setCellValue("");
                    row.createCell(8).setCellValue("");
                    row.createCell(9).setCellValue("");
                    break;
                }
                case 3:{
                    row.createCell(0).setCellValue("");
                    row.createCell(1).setCellValue("ugc");
                    row.createCell(2).setCellValue("");
                    row.createCell(3).setCellValue("");
                    row.createCell(4).setCellValue("");
                    row.createCell(5).setCellValue("");
                    row.createCell(6).setCellValue("");
                    row.createCell(7).setCellValue("");
                    row.createCell(8).setCellValue("");
                    row.createCell(9).setCellValue("");
                    break;
                }
                case 4:{
                    row.createCell(0).setCellValue("");
                    row.createCell(1).setCellValue("总计");
                    row.createCell(2).setCellValue("");
                    row.createCell(3).setCellValue("");
                    row.createCell(4).setCellValue("");
                    row.createCell(5).setCellValue("");
                    row.createCell(6).setCellValue("");
                    row.createCell(7).setCellValue("");
                    row.createCell(8).setCellValue("");
                    row.createCell(9).setCellValue("");
                    break;
                }
                case 5:{
                    row.createCell(0).setCellValue("");
                    row.createCell(1).setCellValue("pgc");
                    row.createCell(2).setCellValue("");
                    row.createCell(3).setCellValue("");
                    row.createCell(4).setCellValue("");
                    row.createCell(5).setCellValue("");
                    row.createCell(6).setCellValue("");
                    row.createCell(7).setCellValue("");
                    row.createCell(8).setCellValue("");
                    row.createCell(9).setCellValue("");
                    break;
            }
            case 6:{
                row.createCell(0).setCellValue("");
                row.createCell(1).setCellValue("ugc");
                row.createCell(2).setCellValue("");
                row.createCell(3).setCellValue("");
                row.createCell(4).setCellValue("");
                row.createCell(5).setCellValue("");
                row.createCell(6).setCellValue("");
                row.createCell(7).setCellValue("");
                row.createCell(8).setCellValue("");
                row.createCell(9).setCellValue("");
                break;
            }
            case 7:{
                row.createCell(0).setCellValue("");
                row.createCell(1).setCellValue("总计");
                row.createCell(2).setCellValue("");
                row.createCell(3).setCellValue("");
                row.createCell(4).setCellValue("");
                row.createCell(5).setCellValue("");
                row.createCell(6).setCellValue("");
                row.createCell(7).setCellValue("");
                row.createCell(8).setCellValue("");
                row.createCell(9).setCellValue("");
                break;
            }case 8:{
                row.createCell(0).setCellValue("");
                row.createCell(1).setCellValue("pgc");
                row.createCell(2).setCellValue("");
                row.createCell(3).setCellValue("");
                row.createCell(4).setCellValue("");
                row.createCell(5).setCellValue("");
                row.createCell(6).setCellValue("");
                row.createCell(7).setCellValue("");
                row.createCell(8).setCellValue("");
                row.createCell(9).setCellValue("");
                break;
            }
            case 9:{
                row.createCell(0).setCellValue("");
                row.createCell(1).setCellValue("ugc");
                row.createCell(2).setCellValue("");
                row.createCell(3).setCellValue("");
                row.createCell(4).setCellValue("");
                row.createCell(5).setCellValue("");
                row.createCell(6).setCellValue("");
                row.createCell(7).setCellValue("");
                row.createCell(8).setCellValue("");
                row.createCell(9).setCellValue("");
                break;
            }
            case 10:{
                row.createCell(0).setCellValue("");
                row.createCell(1).setCellValue("总计");
                row.createCell(2).setCellValue("");
                row.createCell(3).setCellValue("");
                row.createCell(4).setCellValue("");
                row.createCell(5).setCellValue("");
                row.createCell(6).setCellValue("");
                row.createCell(7).setCellValue("");
                row.createCell(8).setCellValue("");
                row.createCell(9).setCellValue("");
                break;
            }
            }
            sheet1.addMergedRegion(new CellRangeAddress(1,4,0,0));
            sheet1.getRow(1).createCell(0).setCellValue("");
        }
    }

    public static void setTime()
    {
        dateMap.add("2017-08-23");
        dateMap.add("2017-08-24");
        dateMap.add("2017-08-25");
        dateMap.add("2017-08-26");
        dateMap.add("2017-08-27");
        dateMap.add("2017-08-28");
        dateMap.add("2017-08-29");
    }
}
