package com.scistor.process.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.SAXValidator;
import org.dom4j.io.XMLWriter;
import org.dom4j.util.XMLErrorHandler;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;

/**
 * @description xml 操作类
 * @author zhujiulong
 * @date 2016年7月25日 下午2:50:20
 *
 */
public class XMLHelper {

	private static final Log LOG= LogFactory.getLog(XMLHelper.class);
	private static final String TASK_XSD="conf/task.xsd";

	/**
	 * 对 task xml 进行 xsd 校验
	 * @param xmlContent
	 * @return sucess=true, failed=false
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws UnsupportedEncodingException
	 */
	public static boolean TASKXMLValidator(String xmlContent) throws ParserConfigurationException, SAXException, UnsupportedEncodingException{

		XMLErrorHandler errorHandler=new XMLErrorHandler();
		SAXParserFactory saxFactory=SAXParserFactory.newInstance();
		saxFactory.setValidating(true);
		saxFactory.setNamespaceAware(true);
		SAXParser parser=saxFactory.newSAXParser();
		SAXReader reader=new SAXReader();
		
		Document document;
		try {
			document = reader.read(new ByteArrayInputStream(xmlContent.getBytes("utf-8")));
		} catch (Exception e) {
			LOG.error("TASK XML Validate Failed");
			LOG.error(e);
			return false;
		}

		parser.setProperty(
                 "http://java.sun.com/xml/jaxp/properties/schemaLanguage",
                 "http://www.w3.org/2001/XMLSchema");
		parser.setProperty(
	             "http://java.sun.com/xml/jaxp/properties/schemaSource",
	             "file:" + TASK_XSD);

		SAXValidator validator = new SAXValidator(parser.getXMLReader());
		validator.setErrorHandler(errorHandler);
		validator.validate(document);
		// writer for debug
		XMLWriter writer = new XMLWriter(OutputFormat.createPrettyPrint());
		if(errorHandler.getErrors().hasContent()){
			try {
				writer.write(errorHandler.getErrors());
			} catch (IOException e) {
				LOG.info(e);
			}finally{
				try {
					writer.close();
				} catch (IOException e) {
					LOG.info(e);
				}
			}
	         return false;
	     }
		return true;
	}
	
	public static void main(String[] args) throws Exception{
		String xmlContent="D:\\x.txt";
		InputStream fis=new FileInputStream(xmlContent);
		byte[] b=new byte[fis.available()];
		fis.read(b);
		String info=new String(b,"utf-8");
		fis.close();
		System.out.println(XMLHelper.TASKXMLValidator(info));
	}
	
}
