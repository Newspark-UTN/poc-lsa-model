import scala.io.Source

val lines = Source.fromInputStream(getClass.getResourceAsStream("/realdata/2016-08-15.json")).getLines.toList

val separators = List("title", "content", "published", "author", "link", "feed", "source", "name", "link", "contenidoNota")

// change all ' characters to " and put appropriate "" on fields
val news = lines.map(line => separators.find(line contains).map(sep => line.replaceAll(s"$sep", s""""$sep"""")).getOrElse(line).replaceAll("[\'“’”]", "\"")).grouped(10).toList

val grouped = news.map(lines => lines.fold("")(_ + _)

grouped foreach println