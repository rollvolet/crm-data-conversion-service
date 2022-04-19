# coding: utf-8
require 'tiny_tds'

DCT = RDF::Vocabulary.new("http://purl.org/dc/terms/")
SCHEMA = RDF::Vocabulary.new("http://schema.org/")
PROV = RDF::Vocabulary.new("http://www.w3.org/ns/prov#")
CRM = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/crm/")
PRICE = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/pricing/")
VCARD = RDF::Vocabulary.new("http://www.w3.org/2006/vcard/ns#")
NCAL = RDF::Vocabulary.new("http://www.semanticdesktop.org/ontologies/2007/04/02/ncal#")

BASE_URI = 'http://data.rollvolet.be/%{resource}/%{id}'
BOOLEAN_DT = RDF::URI('http://mu.semte.ch/vocabularies/typed-literals/boolean')

OUTPUT_FOLDER = '/data'
ROLLVOLET_GRAPH = 'http://mu.semte.ch/graphs/rollvolet'

def create_sql_client
  client = TinyTds::Client.new username: 'sa', password: ENV['SQL_PASSWORD'], host: 'sql-database', database: 'Klanten'
  Mu.log.info "Connected to SQL database" if client.active?
  client
end

def write_graph(filename, graph)
  file_path = File.join(OUTPUT_FOLDER, "#{filename}.ttl")
  Mu.log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
  File.open("#{OUTPUT_FOLDER}/#{filename}.graph", "w+") { |f| f.puts(ROLLVOLET_GRAPH) }
end

require_relative 'conversions/utils'
require_relative 'conversions/codelists'
require_relative 'conversions/offerlines'
require_relative 'conversions/invoicelines'
require_relative 'conversions/telephones'
require_relative 'conversions/calendar-events'

post '/legacy-calculation-lines' do
  sql_client = create_sql_client()
  calculation_line_per_offerline(sql_client)
  status 204
end

post '/offerlines-to-triplestore' do
  sql_client = create_sql_client()
  offerlines_to_triplestore(sql_client)
  status 204
end

post '/invoicelines-to-triplestore' do
  sql_client = create_sql_client()
  invoicelines_to_triplestore(sql_client)
  status 204
end

post '/supplements-to-triplestore' do
  sql_client = create_sql_client()
  supplements_to_triplestore(sql_client)
  status 204
end

post '/telephones-to-triplestore' do
  sql_client = create_sql_client()
  telephones_to_triplestore(sql_client)
  status 204
end

post '/request-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  request_calendar_events_to_triplestore(sql_client)
  status 204
end

post '/intervention-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  intervention_calendar_events_to_triplestore(sql_client)
  status 204
end

post '/order-calendar-events-to-triplestore' do
  sql_client = create_sql_client()
  order_calendar_events_to_triplestore(sql_client)
  status 204
end
