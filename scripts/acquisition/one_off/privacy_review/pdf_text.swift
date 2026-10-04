// Extract text from each PDF path given on stdin and print one JSON line per file: path, page count, text (UTF-8).
// Used only by document_scan.py, which counts pattern shapes and never stores the text.
import Foundation
import PDFKit

while let line = readLine() {
    let url = URL(fileURLWithPath: line)
    var record: [String: Any] = ["path": line]
    if let document = PDFDocument(url: url) {
        record["pages"] = document.pageCount
        record["text"] = document.string ?? ""
        record["locked"] = document.isLocked
        let attributes = document.documentAttributes ?? [:]
        record["has_author"] = attributes[PDFDocumentAttribute.authorAttribute] != nil
    } else {
        record["error"] = "unreadable"
    }
    if let data = try? JSONSerialization.data(withJSONObject: record), let text = String(data: data, encoding: .utf8) {
        print(text)
    }
}
