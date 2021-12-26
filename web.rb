require 'tiny_tds'
require 'linkeddata'

DCT = RDF::Vocabulary.new("http://purl.org/dc/terms/")
SCHEMA = RDF::Vocabulary.new("http://schema.org/")
CRM = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/crm/")

BASE_URI = 'http://data.rollvolet.be/%{resource}/%{id}'

OUTPUT_FOLDER = '/data'

client = TinyTds::Client.new username: 'sa', password: ENV['SQL_PASSWORD'], host: 'sql-database', database: 'Klanten'
log.info "Connected to SQL database" if client.active?

def calculation_line_per_offerline client
  graph = RDF::Graph.new

  offerlines = client.execute("SELECT l.Id, l.Amount FROM TblOfferline l")
  count = 0
  offerlines.each_with_index do |offerline, i|
    uuid = generate_uuid()
    calc_line_uri = RDF::URI(BASE_URI % { :resource => 'calculation-lines', :id => uuid })
    offerline_uri = RDF::URI(BASE_URI % { :resource => 'offerlines', :id => offerline["Id"] })
    amount = RDF::Literal.new(BigDecimal(offerline["Amount"].to_s))

    graph << RDF.Statement(calc_line_uri, RDF.type, CRM.CalculationLine)
    graph << RDF.Statement(calc_line_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(calc_line_uri, DCT.isPartOf, offerline_uri)
    graph << RDF.Statement(calc_line_uri, SCHEMA.amount, amount)
    count = i
  end

  log.info "Generated #{count} calculation lines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-calculation-lines.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

post '/legacy-calculation-lines' do
  calculation_line_per_offerline(client)
  status 204
end
