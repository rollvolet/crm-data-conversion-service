def format_decimal(number)
  if number then sprintf("%0.2f", number).gsub(/(\d)(?=\d{3}+\.)/, '\1 ').gsub(/\./, ',') else '' end
end
