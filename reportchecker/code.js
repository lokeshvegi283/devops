function doPost(e) {
  var data = JSON.parse(e.postData.contents);
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  sheet.clear();

  // Set Headers
  var headers = ["Server IP", "Hostname", "Order Logger", "Trade Archive", "_Agg Trades", "Status", "MTM Mismatches"];
  sheet.appendRow(headers);
  sheet.getRange(1, 1, 1, headers.length).setBackground("#808080").setFontColor("white").setFontWeight("bold");

  var rows = [];
  var backgrounds = [];

  // Parse Data and Colors
  data.results.forEach(function(r) {
    rows.push([r.ip, r.host, r.log_trades, r.arc_trades, r.agg_trades, r.status, r.mtm_mismatches]);
    
    var color = "#d4edda"; // Green
    if (r.status === "Generating...") {
      color = "#fff3cd"; // Yellow
    } else if (r.status.includes("Missing") || r.status.includes("Failed") || r.mtm_mismatches > 0) {
      color = "#f8d7da"; // Red
    }
    backgrounds.push([color, color, color, color, color, color, color]);
  });

  // Write to Sheet
  if (rows.length > 0) {
    sheet.getRange(2, 1, rows.length, headers.length).setValues(rows).setBackgrounds(backgrounds);
    sheet.autoResizeColumns(1, 7);
  }

  // Send Google Chat Card
  var sheetUrl = SpreadsheetApp.getActiveSpreadsheet().getUrl();
  var payload = {
    "cardsV2": [{
      "cardId": "report_card",
      "card": {
        "header": {
          "title": data.report_type + " Validation",
          "subtitle": "Date: " + data.date
        },
        "sections": [{
          "widgets": [
            { "textParagraph": { "text": "Report processed for <b>" + data.results.length + "</b> servers." } },
            { "buttonList": { "buttons": [{ "text": "View Colored Table", "onClick": { "openLink": { "url": sheetUrl } } }] } }
          ]
        }]
      }
    }]
  };

  UrlFetchApp.fetch(data.webhook_url, {
    "method": "post",
    "contentType": "application/json",
    "payload": JSON.stringify(payload)
  });

  return ContentService.createTextOutput("Success");
}
