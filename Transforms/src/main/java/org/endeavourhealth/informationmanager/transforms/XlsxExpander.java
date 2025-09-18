package org.endeavourhealth.informationmanager.transforms;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.endeavourhealth.informationmanager.transforms.sources.SnomedImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Objects;

public class XlsxExpander {
	private static final Logger LOG = LoggerFactory.getLogger(XlsxExpander.class);

	public void exportXlsToTxt(String inputDirectory) throws IOException {
		Path inputDir = Paths.get(inputDirectory);
		for (File fileEntry : Objects.requireNonNull(inputDir.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xlsx")) {

					try (FileInputStream fis = new FileInputStream(fileEntry);
							 Workbook workbook = new XSSFWorkbook(fis)) {
						Sheet sheet = workbook.getSheetAt(0); // first (and only) worksheet
						String outputFileName = fileEntry.getName().replace(".xlsx", ".txt");
						Path outputFile = inputDir.resolve(outputFileName);
						try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
							boolean start=false;
							int rowCount = 0;
							for (Row row : sheet) {
								rowCount++;
								Cell headerCell = row.getCell(0);
								if (getCellValue(headerCell).equals("Coding ID")) {
									start = true;
									continue;
								}
								if (start) {
									StringBuilder sb = new StringBuilder();
									for (Cell cell : row) {
										if (!sb.isEmpty()) sb.append('\t'); // tab separator
										sb.append(getCellValue(cell));
									}
									writer.write(sb.toString());
									writer.newLine();
								}
							}

						}
					}
				}
			}
		}
	}

	private static String getCellValue(Cell cell) {
		switch (cell.getCellType()) {
			case CellType.STRING:
				return cell.getStringCellValue();
			case CellType.NUMERIC:
				if (DateUtil.isCellDateFormatted(cell)) {
					return cell.getDateCellValue().toString();
				} else {
					return String.valueOf(cell.getNumericCellValue());
				}
			case CellType.BOOLEAN:
				return String.valueOf(cell.getBooleanCellValue());
			case CellType.FORMULA:
				return cell.getCellFormula();
			case CellType.BLANK:
				return "";
			default:
				return "";
		}
	}
}
