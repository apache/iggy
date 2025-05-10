# set up Doxygen for API doc generation
if(BUILD_DOCS)
  find_package(Doxygen)

  if(DOXYGEN_FOUND)
      # Note the option ALL which allows to build the docs together with the application
      set(DOXYGEN_CFG ${CMAKE_CURRENT_SOURCE_DIR}/../Doxyfile)
      add_custom_target( docs ALL
          COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_CFG}
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../
          COMMENT "Generating API documentation with Doxygen"
          VERBATIM )
  else()
    message(FATAL_ERROR, "Doxygen need to be installed to generate the doxygen documentation")
  endif()
endif()
