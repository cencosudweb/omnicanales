/**
 *@name Consulta.java
 * 
 *@version 1.0 
 * 
 *@date 30-03-2017
 * 
 *@author EA7129
 * 
 *@copyright Cencosud. All rights reserved.
 */
package cl.org.is.api.job;

import java.io.BufferedWriter;
import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
import java.io.FileWriter;
import java.math.BigDecimal;
//import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipOutputStream;
/**
 * @description 
 */
public class Omnicanal {
	
	private static final int DIFF_HOY_FECHA_INI = 1;
	private static final int DIFF_HOY_FECHA_FIN = 1;
	//private static final int FORMATO_FECHA_0 = 0;
	//private static final int FORMATO_FECHA_1 = 1;
	//private static final int FORMATO_FECHA_3 = 3;
	private static final int FORMATO_FECHA_4 = 4;
	private final static double fB = 1024.0;
	
	//private static final String RUTA_ENVIO = "\\\\172.18.149.154\\datamart";//
	private static final String RUTA_ENVIO = "";//
	

	private static BufferedWriter bw;
	private static String path;
	
	//private static NotificacionServices	notificacionServices;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {
			
			

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {
			info("[crearTxt1]Exception:"+e.getMessage());
		}
	}
	
	private static void crearTxt(Map<String, String> mapArguments) {
		// TODO Auto-generated method stub
		Connection dbconnOracle = crearConexionOracle();
		Connection dbconnOracle2 = crearConexionOracle2();
		
		File file1              	= null;
		File file2              	= null;
		//File file3              	= null;
		
		BufferedWriter bw       	= null;
		BufferedWriter bw2      	= null;
		//BufferedWriter bw3       = null;

		PreparedStatement pstmt 	= null;
		PreparedStatement pstmt2 	= null;
		StringBuffer sb         	= null;
		StringBuffer sb2         	= null;
		
		String sFechaIni        	= null;
		String sFechaFin        	= null;
		String sFechaFin2       	= null;
		String secuencia 			= null;
		String secuencia2 			= secuencia();
		byte[] buffer = new byte[1024];
		//boolean log = false;
		//File archivo = null;
		//File dir = null;
		int count = 0;
		
		
		Date now2 = new Date();
		SimpleDateFormat ft2 = new SimpleDateFormat ("dd/MM/YY hh:mm:ss");
		String currentDate2 = ft2.format(now2);
		info("Inicio Programa: " + currentDate2 + "\n");
		
		Date nowPro = new Date();
		SimpleDateFormat ftPro = new SimpleDateFormat ("YYYY-MM-dd HH:mm:ss");
		String currentDatePro = ftPro.format(nowPro);
		
		info("RUTA_ENVIO: " + RUTA_ENVIO + "\n");
		info("buffer: " + buffer + "\n");
		try {

			try {

				sFechaIni = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_INI);
				sFechaFin = restarDias(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);
				sFechaFin2 = restarDias2(mapArguments.get("-f"), DIFF_HOY_FECHA_FIN);


				info("sFechaIni: " + sFechaIni + "\n");
				info("sFechaFin: " + sFechaFin + "\n");
				info("sFecha: " + sFechaFin2 + "\n");
				info("secuencia1: " + secuencia + "\n");
				info("secuencia2: " + secuencia2 + "\n");
				
				Thread.sleep(60);
				info("Pausa para Eliminar contador sleep(60 seg)" + "\n");
				
				//Obtiene secuencia si es igual a 99 se elimina y vuelve a 01
				if (obtenerSecuencia(dbconnOracle2) == 99) {
					elimnarSecuencia(dbconnOracle2," DELETE FROM  SECUENCIA  ");
					//elimnarSecuencia(dbconnOracle2," DELETE FROM  CONTADOR  ");
				}
				
				Thread.sleep(60);
				info("Pausa para Agregar registro contador sleep(60 seg) " + "\n");
				
				//Agrega registro de secuencia
				agregarRegistroSecuencia(dbconnOracle2, currentDatePro, "1", obtenerSecuencia(dbconnOracle2));
				
				
				info("obtenerSecuencia="+obtenerSecuencia( dbconnOracle2) + "\n");
				if(obtenerSecuencia(dbconnOracle2)<10){
					secuencia = String.valueOf("0" + obtenerSecuencia (dbconnOracle2));
				}  else {
					secuencia = String.valueOf(obtenerSecuencia(dbconnOracle2));
				}
				
				

				/*
				if (obtenerSecuencia(dbconnOracle2)>= 1 && obtenerSecuencia(dbconnOracle2) <= 9 ) {
					secuencia = "0" + obtenerSecuencia (dbconnOracle2);
				} else {
					secuencia = String.valueOf(obtenerSecuencia(dbconnOracle2));
				}
				*/
				info("numero de secuencia " + secuencia + "\n");
				
			}
			catch (Exception e) {

				info("error: " + e);
			}
			file1                   	= new File(path + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
			file2                   	= new File(path + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".ctr");
			//file1                   	= new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia  + ".dat");
			//file2                   	= new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".ctr");

			sb = new StringBuffer();
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER  ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER_1 ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",' ' as LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",' ' as MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden   ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE  WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			sb.append(",0 as COSTO_PROMEDIO ");
			sb.append(",PO.tc_purchase_orders_id as NUM_ORDEN ");
			sb.append(",POL.tc_po_line_id as NUM_LINEA ");
			sb.append(",0 as TIPO_ERROR ");
			sb.append(",'SIN ERROR' DESCRIP_ERROR ");
			sb.append("FROM CA14.purchase_orders po  ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL   ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID  AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2  ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts   ON o.do_status = sts.order_status    ");

			sb.append("WHERE ot.order_type_id in(6)  ");
			sb.append("AND PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("16-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("16-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");

			sb.append(" AND o.D_FACILITY_ALIAS_ID IS NULL ");
			sb.append(" AND pol.purchase_orders_line_status <> 940 ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb.append(" UNION ");
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen ");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER  ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",' ' as LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",' ' as MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden   ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE  WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			
			sb.append(",0 as COSTO_PROMEDIO ");
			sb.append(",PO.tc_purchase_orders_id as NUM_ORDEN ");
			sb.append(",POL.tc_po_line_id as NUM_LINEA ");
			sb.append(",0 as TIPO_ERROR ");
			sb.append(",'SIN ERROR' DESCRIP_ERROR ");
			
			
			//sb.append(",'NO' as venta_empresa ");
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL  ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic  ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			
			sb.append("LEFT JOIN CA14.orders o  ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id  AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND ot.order_type_id in(22,42)  "); // --PickUP C&C
			sb.append("AND POL.order_fulfillment_option = '02' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("15-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("15-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND o.O_FACILITY_ALIAS_ID <> o.D_FACILITY_ALIAS_ID "); //-PICKUP
			sb.append("AND pol.purchase_orders_line_status <> 940 ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb.append(" UNION ");
			sb.append("SELECT ' ' as ASNID ");
			sb.append(",PO.ext_purchase_orders_id as SHIPMENT_ID ");
			sb.append(",o.O_FACILITY_ALIAS_ID     as Origen ");
			sb.append(",NVL(o.D_FACILITY_ALIAS_ID,'CLIENTE')     as Destino ");
			sb.append(", PO.tc_purchase_orders_id as BILL_OF_LADING_NUMBER   ");
			sb.append(",ot.order_type as BILL_OF_LADING_NUMBER_1 ");
			sb.append(",O.tc_order_id              as N_Orden_Distribu ");
			sb.append(",POL.sku                    as SKU ");
			sb.append(",POL.tc_po_line_id          as Linea_PO ");
			sb.append(",sum(POL.allocated_qty)          as Cant_Desc_SKU ");
			sb.append(",' ' as LPN_ID ");
			sb.append(",PO.created_dttm            as Fecha ");
			sb.append(",' ' as MANIF_NBR ");
			sb.append(",POS.description            as Est_Orden ");
			sb.append(",POS2.description           as Estado_de_Linea ");
			sb.append(",CASE WHEN POL.order_fulfillment_option = '01' THEN 'BOPIS' WHEN POL.order_fulfillment_option = '02' THEN 'CLICK AND COLLECT' ELSE 'DESP.DOM.' END AS Bopis_CC ");
			
			
			sb.append(",0 as COSTO_PROMEDIO ");
			sb.append(",PO.tc_purchase_orders_id as NUM_ORDEN ");
			sb.append(",POL.tc_po_line_id as NUM_LINEA ");
			sb.append(",0 as TIPO_ERROR ");
			sb.append(",'SIN ERROR' DESCRIP_ERROR ");
			
			
			sb.append("FROM CA14.purchase_orders po ");
			sb.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb.append("AND ot.order_type_id in(42)  "); // PickUP BOPIS
			sb.append("AND POL.order_fulfillment_option = '01' ");
			sb.append("AND PO.created_dttm >= to_date('");
			sb.append(sFechaIni);
			//sb.append("15-08-2017");
			sb.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND PO.created_dttm <= to_date('");
			sb.append(sFechaFin);
			//sb.append("15-08-2017");
			sb.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb.append("AND pol.purchase_orders_line_status <> 940  ");
			sb.append("GROUP BY ");
			sb.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			info("Query : " + sb + "\n");
			
			pstmt = dbconnOracle.prepareStatement(sb.toString());
			ResultSet rs = pstmt.executeQuery();
			bw = new BufferedWriter(new FileWriter(file1));
			//bw3 = new BufferedWriter(new FileWriter(file3));

			/*
			bw.write("ASNID|");
			bw.write("SHIPMENT_ID|");
			bw.write("ORIGEN|");
			bw.write("DESTINO|");
			bw.write("BILL_OF_LADING_NUMBER|");
			bw.write("BILL_OF_LADING_NUMBER_1|");
			bw.write("N_ORDEN_DISTRIBU|");
			bw.write("SKU|");
			bw.write("LINEA_PO|");
			bw.write("CANT_DESC_SKU|");
			bw.write("LPN_ID|");
			bw.write("FECHA|");
			bw.write("MANIF_NBR|");
			bw.write("EST_ORDEN|");
			bw.write("ESTADO_DE_LINEA|");
			bw.write("BOPIS_CC\n");
			*/
			
			
			sb = new StringBuffer();
			
			while (rs.next()){
				count = count +1 ;
				
				bw.write(rs.getString("ASNID") + "|");
				bw.write(rs.getString("SHIPMENT_ID") + "|");
				bw.write(rs.getString("ORIGEN") + "|");
				bw.write(rs.getString("DESTINO") + "|");
				bw.write(rs.getString("BILL_OF_LADING_NUMBER") + "|");
				bw.write(rs.getString("BILL_OF_LADING_NUMBER_1") + "|");
				bw.write(rs.getString("N_ORDEN_DISTRIBU") + "|");
				bw.write(rs.getString("SKU") + "|");
				bw.write(rs.getString("LINEA_PO") + "|");
				bw.write(rs.getString("CANT_DESC_SKU") + "|");
				bw.write(rs.getString("LPN_ID") + "|");
				bw.write(formatDate(rs.getTimestamp("FECHA"), FORMATO_FECHA_4) + "|");
				bw.write(rs.getString("MANIF_NBR") + "|");
				bw.write(rs.getString("EST_ORDEN") + "|");
				bw.write(rs.getString("ESTADO_DE_LINEA") + "|");
				bw.write(rs.getString("BOPIS_CC") + "|");
				bw.write(rs.getString("COSTO_PROMEDIO") + "|");
				bw.write(rs.getString("NUM_ORDEN") + "|");
				bw.write(rs.getString("NUM_LINEA") + "|");
				bw.write(rs.getString("TIPO_ERROR") + "|");
				bw.write(rs.getString("DESCRIP_ERROR") + "\n");
			}
			bw.write(sb.toString());
			
			info("Archivos creado 1." + "\n");
		}
		catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(null, pstmt, bw);
			//cerrarTodo(dbconnOracle, pstmt2, bw2);

		}
		
		info("count: " + count);
		
		
		try {
			
			Thread.sleep(60);
			info("Pausa de (60 seg) para archivo 2 " + "\n");
			
			sb2 = new StringBuffer();
			sb2.append("SELECT TRIM(NVL(TO_CHAR(SUM(1),'9G999G999'),0)) as total from dual  ");
			/*
			sb2.append("SELECT SUM(1) as total  ");
			sb2.append("FROM ( ");
			//sb2.append("SELECT TRIM(NVL(TO_CHAR(SUM(1),'9G999G999'),0)) as total   ");
			sb2.append("SELECT SUM(1) as total   ");
			sb2.append("FROM CA14.purchase_orders po  ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL   ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos  ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID  AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb2.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2  ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts   ON o.do_status = sts.order_status    ");

			sb2.append("WHERE ot.order_type_id in(6)  ");
			sb2.append("AND PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("16-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("16-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");

			sb2.append(" AND o.D_FACILITY_ALIAS_ID IS NULL ");
			sb2.append(" AND pol.purchase_orders_line_status <> 940 ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb2.append(" UNION ALL ");
			//sb2.append("SELECT TRIM(NVL(TO_CHAR(SUM(1),'9G999G999'),0)) as total  ");
			sb2.append("SELECT SUM(1) as total  ");
			sb2.append("FROM CA14.purchase_orders po ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL  ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic  ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			
			sb2.append("LEFT JOIN CA14.orders o  ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id  AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb2.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND ot.order_type_id in(22,42)  "); // --PickUP C&C
			sb2.append("AND POL.order_fulfillment_option = '02' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("15-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("15-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND o.O_FACILITY_ALIAS_ID <> o.D_FACILITY_ALIAS_ID "); //-PICKUP
			sb2.append("AND pol.purchase_orders_line_status <> 940 ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			
			sb2.append(" UNION ALL ");
			//sb2.append("SELECT TRIM(NVL(TO_CHAR(SUM(1),'9G999G999'),0)) as total ");
			sb2.append("SELECT SUM(1) as total ");

			sb2.append("FROM CA14.purchase_orders po ");
			sb2.append("INNER JOIN CA14.PURCHASE_ORDERS_LINE_ITEM POL ON pol.purchase_orders_id = po.purchase_orders_id ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos ON pos.purchase_orders_status = po.purchase_orders_status ");
			sb2.append("INNER JOIN CA14.order_type ot ON ot.order_type_id = po.order_category ");
			sb2.append("INNER JOIN CA14.item_cbo ic ON ic.item_id = POL.SKU_ID ");
			sb2.append("INNER JOIN CA14.order_line_item OLI ON OLI.PURCHASE_ORDER_LINE_NUMBER = POL.TC_PO_LINE_ID AND OLI.mo_line_item_id = POL.purchase_orders_line_item_id AND OLI.ITEM_ID = POL.SKU_ID AND Oli.Is_Cancelled = 0 ");
			sb2.append("LEFT JOIN CA14.orders o ON o.Order_ID = OLI.Order_ID AND o.purchase_order_id =  po.purchase_orders_id AND o.IS_CANCELLED = 0 ");
			sb2.append("INNER JOIN CA14.purchase_orders_status pos2 ON pos2.purchase_orders_status = pol.purchase_orders_line_status ");
			sb2.append("INNER JOIN CA14.DO_status sts ON o.do_status = sts.order_status ");
			sb2.append("WHERE PO.is_purchase_orders_confirmed = '1' ");
			sb2.append("AND ot.order_type_id in(42)  "); // PickUP BOPIS
			sb2.append("AND POL.order_fulfillment_option = '01' ");
			sb2.append("AND PO.created_dttm >= to_date('");
			sb2.append(sFechaIni);
			//sb2.append("15-08-2017");
			sb2.append(" 00:00:01','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND PO.created_dttm <= to_date('");
			sb2.append(sFechaFin);
			//sb2.append("15-08-2017");
			sb2.append(" 23:59:59','DD-MM-YYYY HH24:MI:SS') ");
			sb2.append("AND pol.purchase_orders_line_status <> 940  ");
			sb2.append("GROUP BY ");
			sb2.append("PO.ext_purchase_orders_id,o.O_FACILITY_ALIAS_ID,o.D_FACILITY_ALIAS_ID,PO.tc_purchase_orders_id,O.tc_order_id,POL.tc_po_line_id,POL.sku, ");
			sb2.append("PO.created_dttm, POS.description, POS2.description, POL.order_fulfillment_option,ot.order_type ");
			sb2.append(") ");
			*/
			

			
			info("Query2 : " + sb2 + "\n");
			
			pstmt2 = dbconnOracle.prepareStatement(sb2.toString());
			ResultSet rs2 = pstmt2.executeQuery();
			bw2 = new BufferedWriter(new FileWriter(file2));
			/*
			bw2.write("chi;");
			bw2.write("paris;");
			bw2.write("0;");
			bw2.write("EOM;");
			bw2.write("ctldet;");
			bw2.write("20170313;");
			bw2.write("11;");
			bw2.write("165;");
			bw2.write("0\n");
			*/
			
			//File file3                   = new File(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia2 + ".dat");
			
			//info("Bytes : " + String.valueOf(file1.length()) + "\n");
			info("bytes : " + Omnicanal.getFileSizeISDecimal(file1.getAbsolutePath()) + "\n");
			//info("bytes : " + Omnicanal.getFileSizeISDecimal(file3.getAbsolutePath()) + "\n");
			//info("bytes : " + file3.length() + "\n");
			//info("bytes : " + file1.length() + "\n");

			
			
			sb2 = new StringBuffer();
			while (rs2.next()){
				//bw2.write(rs2.getString("ASNID") + "|");
				bw2.write("chi" + "|");
				bw2.write("paris" + "|");
				bw2.write("0" + "|");
				bw2.write("EOM" + "|");
				bw2.write("ctldet" + "|");
				bw2.write(sFechaFin2 + "|");
				//bw2.write(( Integer.parseInt(secuencia.substring(6, 8)) + 1 )+ "|");
				bw2.write(secuencia + "|");
				bw2.write(count+"|");
				//bw2.write( Redondear(String.valueOf(Redondear( String.valueOf(file2.length()/1024.0), 2)), 0) + "\n");
				bw2.write(  ( (Omnicanal.getFileSizeISDecimal(file1.getAbsolutePath()) + "\n")));
				//bw2.write( String.valueOf(file2.length()) + "\n");
			}
			bw2.write(sb2.toString());
			
			info("Archivos creado 2." + "\n");
			
		} catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnOracle, pstmt2, bw2);

		}
		
		
		/*
		try{
			
			
			
			
			//file1                   = new File(path  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia+ ".dat");
			file1                   = new File(RUTA_ENVIO  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia+ ".dat");
						

			//FileOutputStream fos = new FileOutputStream(path + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".zip");
			FileOutputStream fos = new FileOutputStream(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".zip");

			
			ZipOutputStream zos = new ZipOutputStream(fos);
    		//ZipEntry ze= new ZipEntry(path  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
    		ZipEntry ze= new ZipEntry(RUTA_ENVIO  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");

    		zos.putNextEntry(ze);
    		//FileInputStream in = new FileInputStream(path  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
    		FileInputStream in = new FileInputStream(RUTA_ENVIO  + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
			
    		int len;
    		while ((len = in.read(buffer)) > 0) {
    			zos.write(buffer, 0, len);
    		}
    		in.close();
    		zos.closeEntry();
    		//remember close it
    		zos.close();
    		fos.close();

    		System.out.println("Done");

    	} catch(IOException ex){
    	   ex.printStackTrace();
    	}
		
		
		
		try {
			Thread.sleep(120);
			//eliminarArchivo(path + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
			eliminarArchivo(RUTA_ENVIO + "/" + "chi_paris_0_omni_mov_"+sFechaFin2+"" + "_" + secuencia + ".dat");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		*/
		
		
		
		
		
		//boolean log = false;
		//File archivo = null;
		//File dir = null;
		//String[] ficheros = null;
	    //BufferedWriter bw       = null;
	    
		/*
		try
		{
			InicializarMIS inicializarMIS = new InicializarMIS();
			inicializarMIS.init();
			notificacionServices = new NotificacionServices();
			
			
			// se buscan los correos asociados a esta transaccion
			//List<String> list = getUtilDao().findCorreos(Transacciones.COPIAR_ARCHIVOS);
			List<String> list = new ArrayList<String>();
			list.add("jose.floyd.jose@gmail.com");
			//list.add("alexis.morales@cencosud.cl");
			destinatarios = new String[list.size()];
			destinatarios = list.toArray(destinatarios);
			info("list.size() " +list.size());
			info("destinatarios " +destinatarios);
			info("============="+InicializarMIS.getPropiedad(Constantes.SCHEDULE_DIA));
			
			//Runtime.getRuntime().exec("C:\\PROYECTOS\\C8INVERSIS\\PROC_INV.bat");
			//Runtime.getRuntime().exec(InicializarMIS.getPropiedad(Constantes.KEY_FILE_BATCH) + " "+InicializarMIS.getPropiedad(Constantes.SERVER));
			
			//Verifico si se genero un archivo en la ruta ERR, si se genera un 
			//archivo es porque hubo algun error en el proceso
			
		    	//se envia la notificacion
		    	//No se esta adjuntando el archivo .log
		    	info("adjunto="+adjunto);
		    	info("Transacciones.COPIAR_ARCHIVOS="+Transacciones.COPIAR_ARCHIVOS);
		    	info("destinatarios="+destinatarios);
		    	info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	info("nicializarMIS.getPropiedad(Constantes.NOTIFICACION6)="+InicializarMIS.getPropiedad(Constantes.NOTIFICACION6));
		    	info("Constantes.ASUNTO_CARGA_ARCHIVOS="+Constantes.ASUNTO_CARGA_ARCHIVOS);
		    	info("notificacionServices="+notificacionServices);
		    	notificacionServices.enviarCorreo(Transacciones.COPIAR_ARCHIVOS, destinatarios, "---", Constantes.ASUNTO_CARGA_ARCHIVOS, adjunto, "txt");

		    			    
		} catch (Exception e)
		{
			info("111111111=");
			// Se hay una excepcion de cualquier tipo se notifica que hubo algun error interno
			info(e.getMessage());
			
		
			info("No Se envio Notificacion de correo");
		}
		finally
		{
			if (null != fr && br != null)
			{
				try
				{
					fr.close();
					br.close();
				} catch (IOException e)
				{
					info("Algun error cerrando objetos de lectura de archivos");
					e.printStackTrace();
				}
			}
		}
		*/
		
		info("Fin Programa: " + currentDate2 + "\n");
	}
	
	
	/**
	 * Metodo que elimina un arvhivo 
	 * 
	 * @return void,  no tiene valor de retorno
	 */
	public static void eliminarArchivo(String archivo){

	     File fichero = new File(archivo);
	   
	     if(fichero.delete()){

	          System.out.println("archivo eliminado");
	    
	     } else {
	    	 System.out.println("archivo no eliminado");
	     }

	}  

	/**
	 * Metodo de conexion para MEOMCLP 
	 * 
	 * @return void,  no tiene valor de retorno
	 */
	private static Connection crearConexionOracle() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			//Shareplex
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9.cencosud.corp:1521:MEOMCLP","REPORTER","RptCyber2015");
			
			//El servidor g500603sv0zt corresponde a ProducciÃ³n. por el momento
			//dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603sv0zt.cencosud.corp:1521:MEOMCLP","ca14","Manhattan1234");
		}
		catch (Exception e) {

			info("[crearConexionOracle]error: " + e);
		}
		return dbconnection;
	}
	
	/**
	 * Metodo de conexion para kpiweb 
	 * 
	 * @return void,  no tiene valor de retorno
	 */
	private static Connection crearConexionOracle2() {

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");

			// Comentado por cambio de base de datos. El servidor g500603svcr9
			// corresponde shareplex.
			// dbconnection =
			// DriverManager.getConnection("jdbc:oracle:thin:@g500603svcr9:1521:MEOMCLP","REPORTER","RptCyber2015");

			// El servidor g500603sv0zt corresponde a Producción.
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@172.18.163.15:1521/XE", "kpiweb", "kpiweb");
		} catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}
	
	/**
	 * Metodo que agrega un registro de secuencia
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param String, fecha actual para la insercion a la tabla
	 * @param String, estado actual
	 * @param String, contador de secuencia (se obtiene del valor maximo de la query )
	 * @return void,  no tiene valor de retorno
	 */
	private static void agregarRegistroSecuencia(Connection dbconnection, String fecha, String estado, int cont){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		int data = cont + 1;
		
		try {
			info("[Ini Metodo agregarRegistroSecuencia]" + "\n");
			
			sb = new StringBuffer();
			//sb.append("DELETE FROM BJPROYEC.Q_40REV3 WHERE POJOB = 'PO_CANCEL'");
			sb.append("INSERT INTO SECUENCIA (FECHA, DESCRIPTION, CONTADOR) VALUES ('"+fecha+"', 'contador', "+data+")");
			//sb.append("INSERT INTO CONTADOR (FECHA, DESCRIPTION, CONTADOR) VALUES ('"+fecha+"', 'contador', "+data+")");
			
			
			pstmt        = dbconnection.prepareStatement(sb.toString());
			info("Registros agrados SECUENCIA: "+pstmt.executeUpdate());	

			info("[Fin Metodo agregarRegistroSecuencia]" + "\n");
			
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[Metodo agregarRegistroSecuencia]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}
	
	


	/**
	 * Metodo que cierra la conexion, Procedimintos,  BufferedWriter
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param PreparedStatement, Objeto que representa una instrucción SQL precompilada. 
	 * @return retorna
	 * 
	 */
	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	/**
	 * Metodo que muestra informacion 
	 * 
	 * @param String, texto a mostra
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:" + e.getMessage());
		}
	}

	/**
	 * Metodo que resta dias 
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias2(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}


	/**
	 * Metodo que resta dias 
	 * 
	 * @param String, dia que se resta
	 * @param String, cantidad para restar dias
	 * @return String retorna los dias a restar
	 * 
	 */
	private static String restarDias(String sDia, int iCantDias) {

		String sFormatoIn = "yyyyMMdd";
		String sFormatoOut = "dd-MM-yyyy";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormatoIn);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -iCantDias);
			df.applyPattern(sFormatoOut);
			sDiaAux = df.format(diaAux.getTime());
		}
		catch (Exception e) {

			info("[restarDias]error: " + e);
		}
		return sDiaAux;
	}

	
	/**
	 * Metodo que formatea una fecha 
	 * 
	 * @param String, fecha a formatear
	 * @param String, formato de fecha
	 * @return String retorna el formato de fecha a un String
	 * 
	 */
	private static String formatDate(Date fecha, int iOptFormat) {

		String sFormatedDate = null;
		String sFormat = null;

		try {

			SimpleDateFormat df = null;

			switch (iOptFormat) {

			case 0:
				sFormat = "dd/MM/yy HH:mm:ss,SSS";
				break;
			case 1:
				sFormat = "dd/MM/yy";
				break;
			case 2:
				sFormat = "dd/MM/yy HH:mm:ss";
				break;
			case 3:
				sFormat = "yyyy-MM-dd HH:mm:ss,SSS";
				break;
				
			case 4:
				sFormat = "yyyyMMdd";
				break;
			}
			df = new SimpleDateFormat(sFormat);
			sFormatedDate = df.format(fecha != null ? fecha:new Date(0));

			if (iOptFormat == 0 && sFormatedDate != null) {

				sFormatedDate = sFormatedDate + "000000";
			}
		}
		catch (Exception e) {

			info("[formatDate]Exception:"+e.getMessage());
		}
		return sFormatedDate;
	}
	
	
	/**
	 * Metodo que redondea un valor de tipo String
	 * 
	 * @param String, valor a redondear 
	 * @param String, numero de decimales a mostrar
	 * @return String retorna el valor redondeado a String
	 * 
	 */
	public static  String Redondear(String valor, int decimales) {
		// TODO Auto-generated method stub
		BigDecimal numero;
	    String resultado = "";
	    String signo = "";

	    if ( (valor != null) && (valor.compareTo("") != 0)) {
	      if (valor.indexOf(",") != -1) {
	        valor = valor.replace(',', '.');
	      }
	      if (valor.substring(0, 1).compareTo("-") == 0) {
	        signo = "-";
	        valor = valor.substring(1, valor.length());
	      }
	      numero = new BigDecimal(valor);
	      resultado = numero.setScale(decimales, BigDecimal.ROUND_HALF_UP).toString();
	      return (signo + resultado);
	    }
	    else {
	      return ("");
	    }
	}
	
	/**
	 * Metodo que calcula un numero de secuencia aleatorio del 01 al 99
	 * 
	 * @return String retorna la secuancia de valor String
	 */
	public static  String secuencia() {
		// TODO Auto-generated method stub
		
		int n = 99; // numeros aleatorios
		int k = n; // auxiliar;
		int[] numeros = new int[n];
		int[] resultado = new int[n];
		Random rnd = new Random();
		int res;

		// se rellena una matriz ordenada del 1 al 31(1..n)
		for (int i = 0; i < n; i++) {
			numeros[i] = i + 1;
		}

		for (int i = 0; i < n; i++) {
			res = rnd.nextInt(k);
			resultado[i] = numeros[res];
			numeros[res] = numeros[k - 1];
			k--;

		}

		//Arrays.sort(resultado);
		// se imprime el resultado;
		
		String val = null;
		
		for (int i = 0; i < 1; i++) {
			
			String result = String.format("%02d", resultado[i]  );
			val = String.format("%02d", resultado[i]  );
			info(result);

			//System.out.println(resultado[i]);
			
			
		}
		return val;
		
		
	}
	
	
	/**
     * Devuelve una cadena de texto con el tamaño de archivo en su debida
     * nomenclatura Decimal del Sistema Internacional:
     * "bytes, KB, MB, GB, TB, EB, PB, ZB, YB".
     * Donde bytes es la unidad mas pequeña equivalente a 8 bits.
     * @see <a href="http://es.wikipedia.org/wiki/Kilobyte">WikiES: KB = Kilobyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Megabyte">WikiES: MB = Megabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Gigabyte">WikiES: GB = Gigabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Terabyte">WikiES: TB = Terabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Petabyte">WikiES: MB = Petabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Exabyte">WikiES: EB = Exabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Zettabyte">WikiES: ZB = Zettabyte</a>
     * @see <a href="http://es.wikipedia.org/wiki/Yottabyte">WikiES: YB = Zettabyte</a>
     * @param path Ruta de archivo que se desea verificar
     * @return tamaño en "bytes, KB, MB, GB, TB, EB, PB, ZB, YB"
     */
    public static String getFileSizeISDecimal (String path) {
        File file = new File(path);
        if (file.exists()) {
            double fL = file.length();
            if (fL <= fB) {
                return String.valueOf(fL).concat(" ");
            } else {
            	return String.valueOf(fL).concat(" ");
            	/*
                double sizeKB = getFileSizeInKB(fL);
                if(getFileSizeInKB(fL) <= fB)
                    return String.valueOf(sizeKB).concat(" KB");
                else {
                    double sizeMB = getFileSizeInMB(fL);
                    if(sizeMB <= fB)
                        return String.valueOf(sizeMB).concat(" MB");
                    else {
                        double sizeGB = getFileSizeInGB(fL);
                        if(sizeGB <= fB)
                            return String.valueOf(sizeGB).concat(" GB");
                        else {
                            double sizeTB = getFileSizeInTB(fL);
                            if(sizeTB <= fB)
                                return String.valueOf(sizeTB).concat(" TB");
                            else {
                                double sizePB = getFileSizeInPB(fL);
                                if(sizePB <= 1024)
                                    return String.valueOf(sizePB).concat(" PB");
                                else {
                                    double sizeEB = getFileSizeInEB(fL);
                                    if (sizeEB <= 1024)
                                        return String.valueOf(sizePB).concat(" EB");
                                    else {
                                        double sizeZB = getFileSizeInZB(fL);
                                        if (sizeZB <= 1024) {
                                            return String.valueOf(sizeZB).concat(" ZB");
                                        } else
                                            return String.valueOf(getFileSizeInYB(fL)).concat(" YB");
                                    }
                                }
                            }
                        }
                    }
                }
                */
            }
            
            
        } else {
            throw new java.util.EmptyStackException();
        }
    }
 
    /**
     * Recibe el tamaño de archivo y lo devuelve en Kilobytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Kilobytes
     
    private static double getFileSizeInKB (double f) {
        f = (f/fB);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }*/
 
    /**
     * Recibe el tamaño de archivo y lo devuelve en Megabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Megabytes
     
    private static double getFileSizeInMB (double f) {
        f = f / Math.pow(fB,2);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }
 */
    /**
     * Recibe el tamaño de archivo y lo devuelve en Gigabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Gigabytes
     
    private static double getFileSizeInGB (double f) {
        f = f / Math.pow(fB,3);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }*/
 
    /**
     * Recibe el tamaño de archivo y lo devuelve en Terabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Kilobytes
     
    private static double getFileSizeInTB (double f) {
        f = f / Math.pow(fB,4);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }*/
 
    /**
     * Recibe el tamaño de archivo y lo devuelve en Petabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Petabytes
     
    private static double getFileSizeInPB (double f) {
        f = f / Math.pow(fB,5);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }
 */
    /**
     * Recibe el tamaño de archivo y lo devuelve en Exabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Exabytes
     
    private static double getFileSizeInEB (double f) {
        f = f / Math.pow(fB,5);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }
 */
    /**
     * Recibe el tamaño de archivo y lo devuelve en Zettabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Zettabytes
     
    private static double getFileSizeInZB (double f) {
        f = f / Math.pow(fB,5);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }
 */
    /**
     * Recibe el tamaño de archivo y lo devuelve en Yottabytes
     * @param f para usos públicos el parámetro debe provenir de un objeto
     * double java.io.File.length();
     * @return Tamaño de archivo en Yottabytes
     
    private static double getFileSizeInYB (double f) {
        f = f / Math.pow(fB,6);
        int fs = (int) Math.pow(10,2);
        return Math.rint(f*fs)/fs;
    }
	*/
    
    
    /**
	 * Metodo que ejecuta la eliminacion de registros de las secuencias
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @param String, query para la eliminacion  
	 * @return  
	 * 
	 */
	private static void elimnarSecuencia(Connection dbconnection,  String sql) {
		PreparedStatement pstmt = null;
		try {
			pstmt = dbconnection.prepareStatement(sql);
			info("registros elimnados secuencia:" +  pstmt.executeUpdate() + "\n");
			//System.out.println("registros elimnados Cumplimient : " + pstmt.executeUpdate());
		}
		catch (Exception e) {
			info("[crearPaso7]Exception elimnar Secuencia:" + e.getMessage());
			//e.printStackTrace();
		}
		finally {
			cerrarTodo(null, pstmt, null);
		}
		
	}
    
	
	/**
	 * Metodo que obtiene el numero de secuencia de la tabla
	 * 
	 * @param Connection,  Objeto que representa una conexion a la base de datos
	 * @return String retorna el numero de secuencia
	 * 
	 */
    private static int obtenerSecuencia(Connection dbconnection) {
		StringBuffer sb = null;
		PreparedStatement pstmt = null;
		int retorno = 0;

		try {

			sb = new StringBuffer();
			sb.append("SELECT max( contador )  as contador FROM SECUENCIA ");
			//sb.append("SELECT max( contador )  as contador FROM CONTADOR ");
			
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			//pstmt.setString(1, odEom.replaceFirst ("^0*", ""));
			//pstmt.setInt(2, sku);
			ResultSet rsSelect = pstmt.executeQuery();
			while (rsSelect.next()) {
				retorno = Integer.parseInt(rsSelect.getString("contador"));
			}
			
			info("retorno secuencia = " + retorno + "\n");
		} catch (Exception e) {

			info("[crearPaso7]Exception Obtener Secuencia:" + e.getMessage());
		} finally {
			cerrarTodo(null, pstmt, null);
		}

		return retorno;
	}
	
	

}
