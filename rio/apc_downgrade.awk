{
  if ( match($0, /(.*)cpu:\s*([0-9]+)/, value_split) )
    print sprintf("%scpu: %d", value_split[1], value_split[2] * 2)
  else if ( match($0, /(.*)cpuRequest:\s*([.0-9]+)/, value_split) )
    print sprintf("%scpuRequest: %d", value_split[1], value_split[2] * 2)
  else if ( !match($0, /priorityClassName:/) )
    print $0
}
