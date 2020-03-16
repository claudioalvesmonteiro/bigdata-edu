#=======================================
# DIAGJUV
# Relatorio Educacao
#
# @claudioalvesmonteiro 2020
#=======================================


# importar pacotes
library(dplyr); library(ggplot2)


# layout ggplot
tema_massa <- function (base_size = 12, base_family = "") {
  theme_minimal(base_size = base_size, base_family = base_family) %+replace% 
    theme(axis.text.x = element_text(colour="black", size=11,hjust=.5,vjust=.5,face="plain"),
          axis.text.y = element_text(colour="black", size=11,angle=0,hjust=1,vjust=0,face="plain"), 
          axis.title.x = element_text(colour="black",size=11,angle=0,hjust=.5,vjust=0,face="plain"),
          axis.title.y = element_text(colour="black",size=11,angle=90,hjust=0.5,vjust=0.6,face="plain"),
          title = element_text(colour="black",size=14,angle=0,hjust=.5,vjust=.5,face="plain"))
}

#===============================
# pre-processamento
#===============================

# importar dados
enem2015 = read.csv('C:/Users/DELL/Documents/Consultorias/DIAGJUV/bigdata-edu/data/enem/ENEM_2015_recife.csv')
enem2016 = read.csv('C:/Users/DELL/Documents/Consultorias/DIAGJUV/bigdata-edu/data/enem/ENEM_2016_recife.csv') 
enem2017 = read.csv('C:/Users/DELL/Documents/Consultorias/DIAGJUV/bigdata-edu/data/enem/ENEM_2017_recife.csv')
enem2018 = read.csv('C:/Users/DELL/Documents/Consultorias/DIAGJUV/bigdata-edu/data/enem/ENEM_2018_recife.csv')

# combinar bases
enem = rbind(enem2015, enem2016, enem2017, enem2018)
rm(enem2015, enem2016, enem2017, enem2018)

# remover casos faltantes [alunos que nao fizeram alguma das provas]
enem = enem[complete.cases(enem[,c('NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC', 'NU_NOTA_MT', 'NU_NOTA_REDACAO')]),]

# selecionar apenas jovens e adultos na base
enem = enem[enem$NU_IDADE >= 15 & enem$NU_IDADE <= 59,]

#======================================================================
# nota nas 4 materias jovem (15-29) X Adulto (30-59) media dos 4 anos
#======================================================================

# criar variavel da nota media
enem = mutate(enem, nota_media = (NU_NOTA_CN + NU_NOTA_CH + NU_NOTA_LC + NU_NOTA_MT + NU_NOTA_REDACAO)/5 )

# selecionar apenas 

# criar variavel adulto X jovem
enem = mutate(enem, jovem =  ifelse(NU_IDADE <= 29, 'Jovem', 'Adulto'))


# nota media nos 4 anos por materia 
m1 = aggregate(enem$NU_NOTA_CN, by=list(enem$jovem), mean)%>%mutate(area='Ciências da Natureza')
m2 = aggregate(enem$NU_NOTA_CH, by=list(enem$jovem), mean)%>%mutate(area='Ciências Humanas')
m3 = aggregate(enem$NU_NOTA_LC, by=list(enem$jovem), mean)%>%mutate(area='Linguagens e Códigos')
m4 = aggregate(enem$NU_NOTA_MT, by=list(enem$jovem), mean)%>%mutate(area='Matemática')
m5 = aggregate(enem$NU_NOTA_REDACAO, by=list(enem$jovem), mean)%>%mutate(area='Redação')
df_materias = rbind(m1, m2, m3, m4, m5)

# plotar
ggplot(data=df_materias, aes(x=df_materias$area, y=x, fill=df_materias$Group.1)) +
  geom_bar(stat="identity", position=position_dodge())+
  geom_text(aes(label = round(x,1) ), vjust = -0.5,size = 4, position = position_dodge(width = 1))+
  scale_fill_manual("", values=c("#E69F00", "#5c4963"),  labels = c('Adulto', 'Jovem'))+
  scale_y_continuous(limits = c(0, 800))+
  labs(x = "", y = paste("Nota Média no ENEM"), title = "" )+
  tema_massa()+
  theme(legend.position="top")
  ggsave("notamedia_materias_jovemXadulto.png", path = "results/visualization", width = 9, height = 5, units = "in")


#====================================================
# nota media jovem (15-29) X Adulto (30-59) por ano
#====================================================


# nota media por grupo em cada ano
df_anos = aggregate(enem$nota_media, by=list(enem$NU_ANO, enem$jovem), mean)

# grafico
ggplot(data = df_anos, aes(x = Group.1, y = x)) +
  geom_line(aes(group = Group.2, color = Group.2, linetype = Group.2), size = 1) + 
  geom_label(aes(label = round(x,1) ))+
  labs(x = "", y = 'Nota Média no ENEM' )+
  scale_linetype_manual("", values = c(1, 2)) +
  scale_color_manual("", values=c("#E69F00", "#5c4963"))+
  scale_y_continuous(limits = c(400,700))+
  tema_massa()
ggsave("notamedia_anos_jovemXadulto.png", path = "results/visualization", width = 8, height = 5, units = "in")


#===================================================================================
# nota media jovens preto+pardo X jovens brancos (30-59) media dos 4 anos
#===================================================================================

# selecionar jovens
enem_jovem = enem[enem$jovem == 'Jovem',]

# por raca/cor
df_raca = aggregate(enem$nota_media, by=list(enem$TP_SEXO, enem$TP_COR_RACA), mean)

# reclassificar raca/cor
df_raca = df_raca[df_raca$Group.2 != '0' & df_raca$Group.2 != '6' ,]
df_raca$Group.2[df_raca$Group.2 == '1'] = 'Branca'
df_raca$Group.2[df_raca$Group.2 == '2'] = 'Preta'
df_raca$Group.2[df_raca$Group.2 == '3'] = 'Parda'
df_raca$Group.2[df_raca$Group.2 == '4'] = 'Amarela'
df_raca$Group.2[df_raca$Group.2 == '5'] = 'Indígena'

# reclassificar sexo
df_raca$Group.1 = as.character(df_raca$Group.1)
df_raca$Group.1[df_raca$Group.1 == 'F'] = 'Feminino'
df_raca$Group.1[df_raca$Group.1 == 'M'] = 'Masculino'


# grafico
ggplot(data = df_raca, aes(x = as.character(Group.2), y = x, fill=Group.1)) +
  geom_bar(stat="identity", position=position_dodge())+
  geom_text(aes(label = round(x,1) ), vjust = -0.5,size = 3.5, position = position_dodge(width = 1))+
  labs(x = '', y = 'Nota Média no ENEM' )+
  scale_fill_manual("Sexo", values=c("#E69F00", "#5c4963"))+
  scale_y_continuous(limits = c(0,800))+
  tema_massa()
ggsave("notamedia_racaXsexo_jovem.png", path = "results/visualization", width = 8, height = 5, units = "in")
